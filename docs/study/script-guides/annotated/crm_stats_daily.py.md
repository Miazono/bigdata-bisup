# Аннотированная копия `crm_stats_daily.py`

## 1. Импорты и класс `Config`

```python
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class Config:
    DAG_ID = 'crm_report_daily'
    DB_CONN_ID = 'postgres_medianation'
    INPUT_PATH = '/opt/airflow/dags/input'
```

Что важно увидеть сразу:

- DAG полностью строится на `PythonOperator`, без SQLOperator и без Spark;
- `pandas` нужен только для приема CSV в `stg`;
- connection id practice 2 задан строкой прямо в классе;
- DAG ID не совпадает с именем файла.

`Config` дальше продолжает объявлять схемы и таблицы как class attributes.
Это делает файл длинным, но зато позволяет не повторять строки вида `dm.platform_stats` по всему коду.

## 2. Базовые helper-функции доступа к данным

```python
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
```

Почему этот блок важен:

- `get_postgres_engine()` нужен для `pandas.to_sql`;
- `run_sql()` — единая точка вызова SQL через Airflow hook;
- `read_csv_safe()` заранее валидирует наличие файла и делает причину падения понятной.

Методически это хороший пример разделения на маленькие утилиты перед основной бизнес-логикой.

## 3. Очистка `stg` и универсальная загрузка CSV

```python
def truncate_stg_tables():
    logger = logging.getLogger("airflow.task")
    tables = [
        Config.TBL_STG_CLIENTS, Config.TBL_STG_CAMPAIGNS, 
        Config.TBL_STG_STATS, Config.TBL_STG_MONITOR
    ]
    sql = f"TRUNCATE TABLE {', '.join(tables)};"
    run_sql(sql)
    logger.info("STG tables truncated.")
```

Смысл блока:

- `stg` не историзируется;
- каждый запуск DAG-а начинает с чистого staging-слоя;
- список таблиц собирается в Python, но на очень ограниченном и контролируемом наборе имен.

Теперь универсальный loader:

```python
def load_stg_csv(filename, table_name, **kwargs):
    logger = logging.getLogger("airflow.task")
    
    execution_date = kwargs.get('ds')
    if '{date}' in filename:
        filename = filename.format(date=execution_date)
    
    df = read_csv_safe(filename)
    df = df.astype(str)
    engine = get_postgres_engine()

    schema, table = table_name.split('.')
    df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} rows into {table_name}")
```

Ключевые детали:

- `kwargs.get('ds')` берет Airflow logical date;
- `filename.format(date=execution_date)` связывает имя файла с датой запуска;
- `astype(str)` делает `stg` полностью текстовым приемочным слоем;
- `table_name.split('.')` разбивает `schema.table` для `to_sql`.

Это, пожалуй, самый важный блок для понимания того, как CSV попадают в DWH.

## 4. Обновление справочников `dds`

```python
def load_dds_clients():
    sql_deactivate = f"""
        UPDATE {Config.TBL_DDS_CLIENTS}
        SET is_active = FALSE
        WHERE id NOT IN (SELECT id FROM {Config.TBL_STG_CLIENTS});
    """
    
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
```

Что здесь важно:

- сначала деактивация отсутствующих в текущем входе сущностей;
- потом `upsert` актуального среза;
- `EXCLUDED.*` — стандартный синтаксис PostgreSQL для новых значений при конфликте.

`load_dds_campaigns()` устроена так же, но дополнительно использует `TO_DATE(start_date, 'YYYY-MM-DD')`, потому что дата в `stg` текстовая.

## 5. Общий шаблон фактов: `delete+insert`

```python
def load_dds_facts(sql_delete, sql_insert, **kwargs):
    ds = kwargs.get('ds')
    
    run_sql(sql_delete, params={'date': ds})
    run_sql(sql_insert, params={'date': ds})
```

Это общий helper для обеих фактовых таблиц.
Он полезен тем, что явно показывает главный паттерн:

- сначала удалить срез за дату;
- потом заново его вставить.

Рекламный факт:

```python
def load_fact_advertising(**kwargs):
    sql_del = f"DELETE FROM {Config.TBL_DDS_FACT_AD} WHERE report_date = %(date)s;"
    
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
```

Почему этот фрагмент полезен для чтения:

- параметр `%(date)s` показывает безопасную подстановку параметров через hook;
- `::INT` и `::DECIMAL` показывают типизацию `stg` в момент переноса;
- join с `dds.campaigns` одновременно привязывает факт к измерению и фильтрует некорректные внешние ключи.

`load_fact_site()` устроена симметрично, только работает с `site_monitoring` и `dds.clients`.

## 6. Дневные и месячные витрины

```python
def build_mart_daily(sql_tmpl, **kwargs):
    ds = kwargs.get('ds')
    run_sql(sql_tmpl, params={'date': ds})

def build_mart_monthly(sql_delete, sql_insert, **kwargs):
    ds = kwargs.get('ds')
    run_sql(sql_delete, params={'date': ds})
    run_sql(sql_insert, params={'date': ds})
```

Названия функций звучат как будто они сами вычисляют витрины.
На деле это просто thin wrappers вокруг исполнения SQL-шаблонов.

### `calc_dm_platform()`

```python
AVG(CAST(f.clicks AS DECIMAL) / NULLIF(f.impressions, 0)) * 100 as ctr,
AVG(f.cost / NULLIF(f.clicks, 0)) as cpc,
SUM(f.clicks)
...
ON CONFLICT (report_date, platform) DO UPDATE SET ...
```

Что важно:

- CTR и CPC считаются в SQL, а не в Grafana;
- `NULLIF` защищает от деления на ноль;
- дневной grain поддерживается `ON CONFLICT`.

### `calc_dm_reliability()`

```python
CASE WHEN SUM(f.server_errors) > 0 THEN 'PROBLEM' ELSE 'OK' END
```

Это хороший пример бизнесовой классификации прямо в SQL-слое витрины.

### `calc_dm_kpi()` и `calc_dm_finance()`

Обе месячные витрины сначала удаляют срез месяца, потом вставляют новый.
Это показывает, что в одном DAG одновременно живут два подхода:

- `upsert` по ключу;
- пересборка среза через `delete+insert`.

## 7. DAG definition и граф зависимостей

```python
with DAG(
    Config.DAG_ID,
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:
```

Здесь важны:

- `catchup=False`;
- `max_active_runs=1`, чтобы не было конкурирующих запусков одного DAG-а;
- отсутствие явного расписания.

Ниже объявляются все `PythonOperator`, а затем собирается граф:

```python
t_truncate >> [t_stg_clients, t_stg_campaigns, t_stg_stats, t_stg_monitor]
[t_stg_clients, t_stg_campaigns, t_stg_stats, t_stg_monitor] >> t_dds_clients
t_dds_clients >> t_dds_campaigns
t_dds_campaigns >> t_dds_ad_facts
t_dds_clients >> t_dds_site_facts
t_dds_ad_facts >> [t_dm_kpi, t_dm_platform, t_dm_finance]
t_dds_site_facts >> t_dm_reliability
```

Как это читать:

- после очистки staging загрузки идут параллельно;
- справочники строятся раньше фактов;
- рекламные и site-monitoring факты расходятся в разные ветки;
- часть DM зависит от рекламы, часть — от мониторинга.

Главная мысль по файлу:
`crm_stats_daily.py` — это не просто DAG definition.
Это одновременно и orchestration, и контейнер для всей SQL-логики practice 2.

