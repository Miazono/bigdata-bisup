from airflow import DAG
from common.constants import DBConnections, SparkConnections, Paths
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random
import sys

DB_CONN_ID = DBConnections.DWH
SPARK_CONN_ID = SparkConnections.DEFAULT

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

# Функция генерации данных
def generate_mock_data():
    # Инициализация хука
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # 1. Генерируем клиентов (100 штук)
        clients = []
        for i in range(1, 101): 
            name = f"Client_{i}"
            date = "2024-01-01"
            clients.append((name, date))
        
        from psycopg2.extras import execute_values
        
        # Вставка клиентов
        print(f"Inserting {len(clients)} clients...")
        execute_values(
            cursor, 
            "INSERT INTO raw.clients (name, registration_date) VALUES %s", 
            clients
        )
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ: Читаем реальные ID, которые присвоила база
        print("Fetching generated client IDs...")
        cursor.execute("SELECT id FROM raw.clients")
        # Получаем список ID (например: [1, 2, ..., 100] или [101, 102...])
        real_client_ids = [row[0] for row in cursor.fetchall()]
        
        if not real_client_ids:
            raise ValueError("No clients were inserted, cannot generate orders.")

        # 2. Генерируем заказы (500 штук)
        orders = []
        for _ in range(500): 
            # Берем случайный ID из тех, что РЕАЛЬНО есть в базе
            c_id = random.choice(real_client_ids)
            amount = round(random.uniform(10.0, 500.0), 2)
            date = "2024-02-15"
            orders.append((c_id, amount, date))
            
        print(f"Inserting {len(orders)} orders...")
        execute_values(
            cursor, 
            "INSERT INTO raw.orders (client_id, amount, order_date) VALUES %s", 
            orders
        )
        
        conn.commit()
        print("Data generation completed successfully.")
        
    except Exception as e:
        conn.rollback()
        print(f"Error generating data: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


with DAG('elt_pipeline', default_args=default_args, schedule=None) as dag:

    # 1. Создаем таблицы (DDL) - RAW слой
    # Используем SQLExecuteQueryOperator вместо PostgresOperator
    create_tables = SQLExecuteQueryOperator(
        task_id='create_raw_tables',
        conn_id=DB_CONN_ID,
        sql='sql/create_raw_tables.sql', 
        split_statements=True       # Разрешает выполнение нескольких команд через ;
    )
    
    # Создаем таблицы (DDL) - MART слой
    create_marts = SQLExecuteQueryOperator(
        task_id='create_mart_tables',
        conn_id=DB_CONN_ID,
        sql='sql/create_mart_tables.sql',
        split_statements=True
    )

    # 2. Генерируем данные (Extract & Load)
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_mock_data
    )

    # 3. Обработка в Spark (Transform)
    process_spark = SparkSubmitOperator(
        task_id='process_client_spend',
        conn_id=SPARK_CONN_ID, 
        application=f'{Paths.SPARK_APPS}/process_data.py', 
        jars=f'{Paths.SPARK_JARS}/postgresql-42.7.3.jar',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        driver_memory='1g',
        name='airflow_spark_job',
        verbose=True,
        conf={
            # "spark.driver.bindAddress": "0.0.0.0",
            # "spark.driver.host": "airflow-worker", 
            # "spark.driver.port": "33333",
            # "spark.blockManager.port": "33334",
        }
    )

    # Определение порядка выполнения
    create_tables >> create_marts >> generate_data >> process_spark
