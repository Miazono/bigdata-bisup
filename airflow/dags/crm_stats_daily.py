import os
import pandas as pd
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ==========================================
# CONFIGURATION
# ==========================================
class Config:
    DAG_ID = 'crm_report_daily'
    DB_CONN_ID = 'postgres_medianation'
    INPUT_PATH = '/opt/airflow/dags/input'
    
    # Schemas
    SCHEMA_STG = 'stg'
    SCHEMA_DDS = 'dds'
    SCHEMA_DM = 'dm'

    # Table Names
    TBL_STG_CLIENTS = f"{SCHEMA_STG}.clients"
    TBL_STG_CAMPAIGNS = f"{SCHEMA_STG}.campaigns"
    TBL_STG_STATS = f"{SCHEMA_STG}.ad_stats"
    TBL_STG_MONITOR = f"{SCHEMA_STG}.site_monitoring"

    TBL_DDS_CLIENTS = f"{SCHEMA_DDS}.clients"
    TBL_DDS_CAMPAIGNS = f"{SCHEMA_DDS}.campaigns"
    TBL_DDS_FACT_AD = f"{SCHEMA_DDS}.fact_advertising"
    TBL_DDS_FACT_SITE = f"{SCHEMA_DDS}.fact_site_health"

    TBL_DM_KPI = f"{SCHEMA_DM}.client_kpi"
    TBL_DM_PLATFORM = f"{SCHEMA_DM}.platform_stats"
    TBL_DM_RELIABILITY = f"{SCHEMA_DM}.site_reliability"
    TBL_DM_FINANCE = f"{SCHEMA_DM}.agency_finance"

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def get_postgres_engine():
    hook = PostgresHook(postgres_conn_id=Config.DB_CONN_ID)
    return hook.get_sqlalchemy_engine()

def run_sql(sql_query, params=None):
    hook = PostgresHook(postgres_conn_id=Config.DB_CONN_ID)
    hook.run(sql_query, parameters=params)

def read_csv_safe(filename):
    path = os.path.join(Config.INPUT_PATH, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_csv(path)

# ==========================================
# TASKS LOGIC: STAGING
# ==========================================
def truncate_stg_tables():
    logger = logging.getLogger("airflow.task")
    tables = [
        Config.TBL_STG_CLIENTS, Config.TBL_STG_CAMPAIGNS, 
        Config.TBL_STG_STATS, Config.TBL_STG_MONITOR
    ]
    sql = f"TRUNCATE TABLE {', '.join(tables)};"
    run_sql(sql)
    logger.info("STG tables truncated.")

def load_stg_csv(filename, table_name, **kwargs):
    logger = logging.getLogger("airflow.task")
    
    # Обработка динамических имен файлов (с датой)
    execution_date = kwargs.get('ds')
    if '{date}' in filename:
        filename = filename.format(date=execution_date)
    
    df = read_csv_safe(filename)
    
    # Все поля в STG - TEXT. Приводим типы
    df = df.astype(str)
    
    engine = get_postgres_engine()
    # table_name split to schema + table for to_sql
    schema, table = table_name.split('.')
    
    df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} rows into {table_name}")

# ==========================================
# TASKS LOGIC: DDS (DIMENSIONS)
# ==========================================
def load_dds_clients():
    # 1. Помечаем удаленных (is_active = False)
    sql_deactivate = f"""
        UPDATE {Config.TBL_DDS_CLIENTS}
        SET is_active = FALSE
        WHERE id NOT IN (SELECT id FROM {Config.TBL_STG_CLIENTS});
    """
    
    # 2. Вставляем новых или обновляем существующих (Upsert)
    sql_upsert = f"""
        INSERT INTO {Config.TBL_DDS_CLIENTS} (id, name, website, industry, manager, is_active)
        SELECT id, name, website, industry, manager, TRUE
        FROM {Config.TBL_STG_CLIENTS}
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            website = EXCLUDED.website,
            industry = EXCLUDED.industry,
            manager = EXCLUDED.manager,
            is_active = TRUE,
            updated_at = NOW();
    """
    run_sql(sql_deactivate)
    run_sql(sql_upsert)

def load_dds_campaigns():
    # 1. Deactivate
    sql_deactivate = f"""
        UPDATE {Config.TBL_DDS_CAMPAIGNS}
        SET is_active = FALSE
        WHERE id NOT IN (SELECT id FROM {Config.TBL_STG_CAMPAIGNS});
    """
    
    # 2. Upsert
    # Важно: Приводим start_date к типу DATE
    sql_upsert = f"""
        INSERT INTO {Config.TBL_DDS_CAMPAIGNS} (id, client_id, platform, type, start_date, is_active)
        SELECT 
            id, client_id, platform, type, 
            TO_DATE(start_date, 'YYYY-MM-DD'), 
            TRUE
        FROM {Config.TBL_STG_CAMPAIGNS}
        ON CONFLICT (id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            platform = EXCLUDED.platform,
            type = EXCLUDED.type,
            start_date = EXCLUDED.start_date,
            is_active = TRUE,
            updated_at = NOW();
    """
    run_sql(sql_deactivate)
    run_sql(sql_upsert)

# ==========================================
# TASKS LOGIC: DDS (FACTS)
# ==========================================
def load_dds_facts(sql_delete, sql_insert, **kwargs):
    ds = kwargs.get('ds')
    
    # 1. Удаляем данные за текущую дату, чтобы избежать дублей
    run_sql(sql_delete, params={'date': ds})
    
    # 2. Вставляем новые
    run_sql(sql_insert, params={'date': ds})

def load_fact_advertising(**kwargs):
    sql_del = f"DELETE FROM {Config.TBL_DDS_FACT_AD} WHERE report_date = %(date)s;"
    
    # JOIN с dimensions для проверки целостности
    sql_ins = f"""
        INSERT INTO {Config.TBL_DDS_FACT_AD} 
        (report_date, campaign_id, impressions, clicks, cost, conversions)
        SELECT 
            TO_DATE(stg.date, 'YYYY-MM-DD'),
            stg.campaign_id,
            stg.impressions::INT,
            stg.clicks::INT,
            stg.cost::DECIMAL,
            stg.conversions::INT
        FROM {Config.TBL_STG_STATS} stg
        JOIN {Config.TBL_DDS_CAMPAIGNS} d ON stg.campaign_id = d.id
        WHERE stg.date = %(date)s;
    """
    load_dds_facts(sql_del, sql_ins, **kwargs)

def load_fact_site(**kwargs):
    sql_del = f"DELETE FROM {Config.TBL_DDS_FACT_SITE} WHERE check_date = %(date)s;"
    
    sql_ins = f"""
        INSERT INTO {Config.TBL_DDS_FACT_SITE}
        (check_date, client_id, load_time_ms, uptime_pct, server_errors)
        SELECT 
            TO_DATE(stg.date, 'YYYY-MM-DD'),
            stg.client_id,
            stg.load_time_ms::INT,
            stg.uptime_pct::DECIMAL,
            stg.server_errors::INT
        FROM {Config.TBL_STG_MONITOR} stg
        JOIN {Config.TBL_DDS_CLIENTS} d ON stg.client_id = d.id
        WHERE stg.date = %(date)s;
    """
    load_dds_facts(sql_del, sql_ins, **kwargs)

# ==========================================
# TASKS LOGIC: DATA MARTS
# ==========================================
def build_mart_daily(sql_tmpl, **kwargs):
    ds = kwargs.get('ds')
    # Используем ON CONFLICT для идемпотентности
    run_sql(sql_tmpl, params={'date': ds})

def build_mart_monthly(sql_delete, sql_insert, **kwargs):
    ds = kwargs.get('ds')
    # Пересчет месяца
    run_sql(sql_delete, params={'date': ds})
    run_sql(sql_insert, params={'date': ds})

# --- Mart Logic Wrappers ---

def calc_dm_platform(**kwargs):
    sql = f"""
        INSERT INTO {Config.TBL_DM_PLATFORM} (report_date, platform, avg_ctr, avg_cpc, clicks)
        SELECT 
            f.report_date,
            c.platform,
            AVG(CAST(f.clicks AS DECIMAL) / NULLIF(f.impressions, 0)) * 100 as ctr,
            AVG(f.cost / NULLIF(f.clicks, 0)) as cpc,
            SUM(f.clicks)
        FROM {Config.TBL_DDS_FACT_AD} f
        JOIN {Config.TBL_DDS_CAMPAIGNS} c ON f.campaign_id = c.id
        WHERE f.report_date = %(date)s
        GROUP BY 1, 2
        ON CONFLICT (report_date, platform) DO UPDATE SET
            avg_ctr = EXCLUDED.avg_ctr,
            avg_cpc = EXCLUDED.avg_cpc,
            clicks = EXCLUDED.clicks,
            updated_at = NOW();
    """
    build_mart_daily(sql, **kwargs)

def calc_dm_reliability(**kwargs):
    sql = f"""
        INSERT INTO {Config.TBL_DM_RELIABILITY} (report_date, website, avg_load_time, total_errors, status)
        SELECT 
            f.check_date,
            c.website,
            AVG(f.load_time_ms)::INT,
            SUM(f.server_errors),
            CASE WHEN SUM(f.server_errors) > 0 THEN 'PROBLEM' ELSE 'OK' END
        FROM {Config.TBL_DDS_FACT_SITE} f
        JOIN {Config.TBL_DDS_CLIENTS} c ON f.client_id = c.id
        WHERE f.check_date = %(date)s
        GROUP BY 1, 2
        ON CONFLICT (report_date, website) DO UPDATE SET
            avg_load_time = EXCLUDED.avg_load_time,
            total_errors = EXCLUDED.total_errors,
            status = EXCLUDED.status,
            updated_at = NOW();
    """
    build_mart_daily(sql, **kwargs)

def calc_dm_kpi(**kwargs):
    # Удаляем месяц
    sql_del = f"DELETE FROM {Config.TBL_DM_KPI} WHERE report_month = DATE_TRUNC('month', %(date)s::date);"
    
    # Считаем месяц заново
    sql_ins = f"""
        INSERT INTO {Config.TBL_DM_KPI} (report_month, client_name, industry, total_spend, total_conversions, cpl)
        SELECT 
            DATE_TRUNC('month', f.report_date)::DATE,
            c.name,
            c.industry,
            SUM(f.cost),
            SUM(f.conversions),
            SUM(f.cost) / NULLIF(SUM(f.conversions), 0)
        FROM {Config.TBL_DDS_FACT_AD} f
        JOIN {Config.TBL_DDS_CAMPAIGNS} cmp ON f.campaign_id = cmp.id
        JOIN {Config.TBL_DDS_CLIENTS} c ON cmp.client_id = c.id
        WHERE DATE_TRUNC('month', f.report_date) = DATE_TRUNC('month', %(date)s::date)
        GROUP BY 1, 2, 3;
    """
    build_mart_monthly(sql_del, sql_ins, **kwargs)

def calc_dm_finance(**kwargs):
    sql_del = f"DELETE FROM {Config.TBL_DM_FINANCE} WHERE report_month = DATE_TRUNC('month', %(date)s::date);"
    
    sql_ins = f"""
        INSERT INTO {Config.TBL_DM_FINANCE} (report_month, manager, active_clients, turnover, commission)
        SELECT 
            DATE_TRUNC('month', f.report_date)::DATE,
            c.manager,
            COUNT(DISTINCT c.id),
            SUM(f.cost),
            SUM(f.cost) * 0.10
        FROM {Config.TBL_DDS_FACT_AD} f
        JOIN {Config.TBL_DDS_CAMPAIGNS} cmp ON f.campaign_id = cmp.id
        JOIN {Config.TBL_DDS_CLIENTS} c ON cmp.client_id = c.id
        WHERE DATE_TRUNC('month', f.report_date) = DATE_TRUNC('month', %(date)s::date)
        GROUP BY 1, 2;
    """
    build_mart_monthly(sql_del, sql_ins, **kwargs)

# ==========================================
# DAG DEFINITION
# ==========================================
default_args = {
    'owner': 'medianation',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    Config.DAG_ID,
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:

    t_truncate = PythonOperator(
        task_id='truncate_stg',
        python_callable=truncate_stg_tables
    )

    t_stg_clients = PythonOperator(
        task_id='load_stg_clients',
        python_callable=load_stg_csv,
        op_kwargs={'filename': 'clients.csv', 'table_name': Config.TBL_STG_CLIENTS}
    )
    
    t_stg_campaigns = PythonOperator(
        task_id='load_stg_campaigns',
        python_callable=load_stg_csv,
        op_kwargs={'filename': 'campaigns.csv', 'table_name': Config.TBL_STG_CAMPAIGNS}
    )

    t_stg_stats = PythonOperator(
        task_id='load_stg_ad_stats',
        python_callable=load_stg_csv,
        op_kwargs={'filename': 'ad_stats_{date}.csv', 'table_name': Config.TBL_STG_STATS}
    )

    t_stg_monitor = PythonOperator(
        task_id='load_stg_monitor',
        python_callable=load_stg_csv,
        op_kwargs={'filename': 'site_monitoring_{date}.csv', 'table_name': Config.TBL_STG_MONITOR}
    )

    # 3. Load DDS Dimensions (Sequential)
    t_dds_clients = PythonOperator(
        task_id='load_dds_clients',
        python_callable=load_dds_clients
    )
    
    t_dds_campaigns = PythonOperator(
        task_id='load_dds_campaigns',
        python_callable=load_dds_campaigns
    )

    # 4. Load DDS Facts (Parallel)
    t_dds_ad_facts = PythonOperator(
        task_id='load_dds_ad_facts',
        python_callable=load_fact_advertising
    )

    t_dds_site_facts = PythonOperator(
        task_id='load_dds_site_facts',
        python_callable=load_fact_site
    )

    # 5. Build Data Marts (Parallel)
    t_dm_kpi = PythonOperator(
        task_id='build_dm_kpi',
        python_callable=calc_dm_kpi
    )
    
    t_dm_platform = PythonOperator(
        task_id='build_dm_platform',
        python_callable=calc_dm_platform
    )
    
    t_dm_reliability = PythonOperator(
        task_id='build_dm_reliability',
        python_callable=calc_dm_reliability
    )
    
    t_dm_finance = PythonOperator(
        task_id='build_dm_finance',
        python_callable=calc_dm_finance
    )

    
    t_truncate >> [t_stg_clients, t_stg_campaigns, t_stg_stats, t_stg_monitor]
    
    [t_stg_clients, t_stg_campaigns, t_stg_stats, t_stg_monitor] >> t_dds_clients
    
    t_dds_clients >> t_dds_campaigns
    
    t_dds_campaigns >> t_dds_ad_facts
    t_dds_clients >> t_dds_site_facts
    
    t_dds_ad_facts >> [t_dm_kpi, t_dm_platform, t_dm_finance]
    t_dds_site_facts >> t_dm_reliability
