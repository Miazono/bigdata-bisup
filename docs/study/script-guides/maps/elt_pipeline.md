# Карта `elt_pipeline.py`

## Назначение

Файл описывает DAG practice 1.
Он:

- создает `raw`-таблицы;
- создает `mart`-таблицу;
- генерирует mock-данные;
- запускает Spark job.

## Где используется

- Airflow DAG ID: `elt_pipeline`
- Spark application: `spark/apps/process_data.py`
- SQL-файлы: `create_raw_tables.sql`, `create_mart_tables.sql`

## Входы

- connection `dwh_postgres`;
- connection `spark_default`;
- путь `/opt/spark/work-dir/process_data.py`;
- путь `/opt/spark/work-dir/postgresql-42.7.3.jar`.

## Выходы

- `raw.clients`
- `raw.orders`
- `mart.client_spend_report`

## Ключевые шаги

1. Патчится `SparkSubmitHook._resolve_connection`.
2. Выполняется SQL bootstrap для `raw`.
3. Выполняется SQL bootstrap для `mart`.
4. Через `PythonOperator` генерируются 100 клиентов и 500 заказов.
5. Через `SparkSubmitOperator` отправляется Spark job.

## Что важно помнить

- `raw` очищается на каждом запуске.
- `mart` не очищается.
- Spark пишет в `append`.
- Spark JDBC connection в самом job захардкожен, а не берется из Airflow connection.

