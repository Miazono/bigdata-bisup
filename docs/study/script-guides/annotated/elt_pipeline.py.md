# Аннотированная копия `elt_pipeline.py`

## 1. Импорты, connection id и `default_args`

```python
from airflow import DAG
from common.constants import DBConnections, SparkConnections, Paths
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
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
```

Что здесь важно:

- practice 1 берет connection id и пути из `common/constants.py`;
- используются сразу три разных operator/hook-уровня: SQL, Python и Spark;
- `sys` импортирован, но фактически не используется;
- `catchup` записан в `default_args`, а не явно в конструктор DAG, что делает намерение чуть менее очевидным при чтении.

## 2. Monkey patch `SparkSubmitHook`

```python
_original_resolve_connection = SparkSubmitHook._resolve_connection


def _resolve_connection_with_spark_master(self):
    conn_data = _original_resolve_connection(self)
    master = conn_data.get("master")

    if not master or master.startswith(("spark://")):
        return conn_data

    conn = self.get_connection(self._conn_id)

    if conn.host:
        conn_data["master"] = f"spark://{conn.host}:{conn.port}"

    return conn_data


SparkSubmitHook._resolve_connection = _resolve_connection_with_spark_master
```

Смысл блока:

- сохранить стандартное поведение Airflow provider;
- при необходимости дополнить его правильным `spark://host:port`;
- сделать это до объявления задач DAG-а.

Почему блок важен:

- это не обычный конфиг, а прямое изменение поведения класса библиотеки;
- без этой вставки локальная интеграция Airflow и Spark могла бы работать нестабильно;
- при чтении DAG-а нужно воспринимать этот код как инфраструктурный workaround.

## 3. Генерация mock-данных

```python
def generate_mock_data():
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        clients = []
        for i in range(1, 101): 
            name = f"Client_{i}"
            date = "2024-01-01"
            clients.append((name, date))
        
        from psycopg2.extras import execute_values
        
        execute_values(
            cursor, 
            "INSERT INTO raw.clients (name, registration_date) VALUES %s", 
            clients
        )
```

Что стоит видеть в этом фрагменте:

- `PostgresHook.get_conn()` возвращает реальное psycopg2-соединение;
- `execute_values` используется для batch insert, а не для цикла по одному `INSERT`;
- клиенты генерируются с одинаковой датой регистрации;
- practice 1 не читает внешний источник, а полностью синтезирует вход сама.

## 4. Получение `id` клиентов и генерация заказов

```python
        cursor.execute("SELECT id FROM raw.clients")

        client_ids = [row[0] for row in cursor.fetchall()]
        
        if not client_ids:
            raise ValueError("No clients were inserted, cannot generate orders.")

        orders = []
        for _ in range(500): 
            c_id = random.choice(client_ids)
            amount = round(random.uniform(10.0, 500.0), 2)
            date = "2024-02-15"
            orders.append((c_id, amount, date))
```

Почему это важно:

- функция сначала пишет измерение клиентов, потом использует реальные `id` для факта заказов;
- здесь хорошо виден простой учебный pattern «сначала dimension, потом fact»;
- random не фиксируется через seed, поэтому суммы и распределение заказов между клиентами будут немного меняться от запуска к запуску.

## 5. Commit, rollback и закрытие ресурсов

```python
        execute_values(
            cursor, 
            "INSERT INTO raw.orders (client_id, amount, order_date) VALUES %s", 
            orders
        )
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Error generating data: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()
```

Этот блок важен не меньше самой вставки:

- `commit` фиксирует обе партии данных одной транзакцией;
- `rollback` защищает от частично записанного состояния;
- `finally` гарантирует закрытие курсора и соединения.

Для учебного чтения это хороший пример ручного управления транзакцией внутри Airflow task.

## 6. Определение DAG и SQL-задачи

```python
with DAG('elt_pipeline', default_args=default_args, schedule=None) as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id='create_raw_tables',
        conn_id=DB_CONN_ID,
        sql='sql/create_raw_tables.sql', 
        split_statements=True
    )
    
    create_marts = SQLExecuteQueryOperator(
        task_id='create_mart_tables',
        conn_id=DB_CONN_ID,
        sql='sql/create_mart_tables.sql',
        split_statements=True
    )
```

Что важно:

- `with DAG(...) as dag:` — стандартный контекстный стиль объявления DAG;
- `schedule=None` делает DAG ручным;
- `split_statements=True` обязателен, потому что SQL-файлы содержат несколько команд.

## 7. Python-task и Spark-task

```python
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_mock_data
    )

    process_spark = SparkSubmitOperator(
        task_id='process_client_spend',
        conn_id=SPARK_CONN_ID, 
        application=f'{Paths.SPARK_APPS}/process_data.py', 
        jars=f'{Paths.SPARK_JARS}/postgresql-42.7.3.jar',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        driver_memory='1g',
        deploy_mode='client',
        name='airflow_spark_job',
        verbose=True,
        conf={}
    )
```

Здесь особенно полезно замечать:

- Airflow orchestration последовательно передает управление сначала Python, потом Spark;
- `application` и `jars` берутся из общего mounted каталога;
- `deploy_mode='client'` означает, что driver живет со стороны запуска;
- `conf` сейчас пустой, но оставлен как место для дополнительных настроек Spark.

## 8. Граф зависимостей

```python
    create_tables >> create_marts >> generate_data >> process_spark
```

Это короткая строка, но она задает весь pipeline practice 1:

- сначала подготовить структуру;
- затем сгенерировать данные;
- затем отдать вычисления Spark.

Главная мысль при чтении файла:
`elt_pipeline.py` не пытается быть сложным DWH-DAG-ом.
Это компактный учебный маршрут, который показывает интеграцию трех типов задач в одном orchestration-контуре.

