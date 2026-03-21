# Практика 1: пайплайн `elt_pipeline`

## Какие файлы формируют эту практику

Практика 1 собирается из четырех основных артефактов:

- `airflow/dags/elt_pipeline.py`;
- `airflow/dags/sql/create_raw_tables.sql`;
- `airflow/dags/sql/create_mart_tables.sql`;
- `spark/apps/process_data.py`.

Дополнительно на нее влияют:

- `airflow/dags/common/constants.py`;
- `airflow/scripts/init_db.sql`;
- `airflow/docker-compose.yaml`;
- `spark/docker-compose.yaml`.

## Общая идея пайплайна

Это самый короткий и самый учебный контур в репозитории.
Он показывает схему:

`Airflow -> PostgreSQL raw -> Spark -> PostgreSQL mart`

По факту пайплайн делает не общую ETL-платформу, а один конкретный отчет:

- сгенерировать клиентов и заказы;
- посчитать суммарные траты по клиентам;
- записать итог в витрину `mart.client_spend_report`.

## Как устроен DAG

В `elt_pipeline.py` DAG состоит из четырех задач:

1. `create_raw_tables`
2. `create_mart_tables`
3. `generate_data`
4. `process_client_spend`

Зависимости линейные:

`create_raw_tables >> create_mart_tables >> generate_data >> process_client_spend`

Это означает, что параллелизма внутри practice 1 нет.
Каждый шаг ждет завершения предыдущего.

## Connections и константы

Из `common/constants.py` берутся:

- `DBConnections.DWH = "dwh_postgres"`;
- `SparkConnections.DEFAULT = "spark_default"`;
- `Paths.SPARK_APPS = "/opt/spark/work-dir"`;
- `Paths.SPARK_JARS = "/opt/spark/work-dir"`.

Это соответствует connection env в `airflow/docker-compose.yaml`:

- `AIRFLOW_CONN_DWH_POSTGRES`;
- `AIRFLOW_CONN_SPARK_DEFAULT`.

Но важно видеть архитектурную особенность:

- имя `dwh_postgres` звучит как отдельное хранилище;
- на деле connection ведет в сервис `postgres`, где одновременно лежат и metadata Airflow, и таблицы practice 1.

## Шаг 1. `create_raw_tables`

Задача `create_raw_tables` использует `SQLExecuteQueryOperator` и выполняет файл `sql/create_raw_tables.sql`.

SQL делает три вещи:

1. создает схему `raw`, если ее еще нет;
2. создает таблицы `raw.clients` и `raw.orders`;
3. очищает обе таблицы перед новой загрузкой.

### `raw.clients`

Поля:

- `id SERIAL PRIMARY KEY`
- `name VARCHAR(100)`
- `registration_date DATE`

Смысл строки: один клиент.

### `raw.orders`

Поля:

- `id SERIAL PRIMARY KEY`
- `client_id INT`
- `amount DECIMAL(10, 2)`
- `order_date DATE`
- `FOREIGN KEY (client_id) REFERENCES raw.clients(id)`

Смысл строки: один заказ.

### Почему в конце идет `TRUNCATE`

Файл завершает создание таблиц командой:

- `TRUNCATE TABLE raw.orders RESTART IDENTITY CASCADE;`
- `TRUNCATE TABLE raw.clients RESTART IDENTITY CASCADE;`

Это делает practice 1 идемпотентной на уровне `raw`.
Каждый новый прогон пересобирает входные таблицы с нуля.

## Шаг 2. `create_mart_tables`

Задача `create_mart_tables` тоже использует `SQLExecuteQueryOperator` и файл `sql/create_mart_tables.sql`.

Этот SQL:

- создает схему `mart`;
- создает таблицу `mart.client_spend_report`.

### Структура `mart.client_spend_report`

Поля:

- `client_name VARCHAR(100)`
- `total_spent DECIMAL(12, 2)`
- `generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

Смысл строки: одна строка на одного клиента в рамках одного конкретного расчета отчета.

### Важная деталь

В отличие от `raw`, эта таблица:

- не очищается в SQL-файле;
- не имеет первичного ключа;
- не имеет уникального ограничения;
- не защищена от повторных вставок одного и того же набора данных.

Это станет главным ограничением practice 1.

## Шаг 3. `generate_data`

Задача `generate_data` использует `PythonOperator` и функцию `generate_mock_data()`.

### Как работает функция

1. Создается `PostgresHook(postgres_conn_id=DB_CONN_ID)`.
2. Через `get_conn()` получается реальное psycopg2-соединение.
3. Открывается cursor.
4. Формируется список клиентов.
5. Импортируется `psycopg2.extras.execute_values`.
6. Пакетно вставляются клиенты.
7. Считываются реальные `id` из `raw.clients`.
8. На их основе формируется список заказов.
9. Пакетно вставляются заказы.
10. Выполняется `commit`.

Если что-то падает, делается `rollback`.

### Какие данные генерируются

Клиенты:

- 100 строк;
- имена вида `Client_1`, `Client_2`, ...;
- одинаковая `registration_date = '2024-01-01'`.

Заказы:

- 500 строк;
- `client_id` случайно выбирается из реально вставленных клиентов;
- `amount` — случайное число от 10.0 до 500.0;
- `order_date = '2024-02-15'` у всех заказов.

### Почему это важно

Этот шаг показывает сразу несколько вещей:

- использование `PostgresHook` не только для `run`, но и для raw connection;
- применение `execute_values` для batch insert;
- зависимость второй вставки от первой через чтение уже вставленных `id`.

Но с точки зрения реалистичности данные предельно упрощены.
Здесь нет разных периодов, статусов, дублирующихся клиентов, плохих записей и других реальных проблем источника.

## Шаг 4. `process_client_spend`

Последняя задача использует `SparkSubmitOperator`.
Параметры важны сами по себе:

- `application = /opt/spark/work-dir/process_data.py`;
- `jars = /opt/spark/work-dir/postgresql-42.7.3.jar`;
- `deploy_mode = 'client'`;
- `total_executor_cores = '1'`;
- `executor_cores = '1'`;
- `executor_memory = '1g'`;
- `driver_memory = '1g'`.

Это очень маленькая локальная Spark job, настроенная под учебный стенд.

## Зачем в DAG есть monkey patch `SparkSubmitHook`

В начале файла сохраняется оригинальная функция:

- `_original_resolve_connection = SparkSubmitHook._resolve_connection`

После этого определяется обертка `_resolve_connection_with_spark_master`, которая:

- берет стандартный `conn_data`;
- проверяет поле `master`;
- если нужно, берет host и port из Airflow connection;
- собирает строку `spark://{host}:{port}`.

Потом эта функция подменяет оригинальную:

- `SparkSubmitHook._resolve_connection = _resolve_connection_with_spark_master`

С практической точки зрения это патч для того, чтобы `SparkSubmitOperator` стабильно получил мастер в нужном формате внутри локального контейнерного контура.

## Что делает `spark/apps/process_data.py`

Spark job очень короткий, но показательный.

### Шаг 1. Создание SparkSession

`SparkSession.builder.appName("ClientOrdersReport").getOrCreate()`

Это точка входа в Spark-приложение.

### Шаг 2. Настройка JDBC

В скрипте жестко заданы:

- `jdbc_url = "jdbc:postgresql://postgres:5432/airflow"`
- `user = airflow`
- `password = airflow`
- `driver = org.postgresql.Driver`

Это важная архитектурная деталь:

- Airflow сам использует connection `dwh_postgres`;
- Spark job не читает этот connection из Airflow;
- он использует hardcoded JDBC URL и credentials.

То есть конфигурация practice 1 разнесена между Airflow connection и самим Spark script.

### Шаг 3. Чтение raw-таблиц

Spark читает:

- `raw.clients`;
- `raw.orders`.

Обе таблицы читаются как DataFrame через `spark.read.jdbc(...)`.

### Шаг 4. Join и агрегация

Далее выполняется:

- `orders_df.join(clients_df, orders_df.client_id == clients_df.id, "inner")`
- `groupBy("name")`
- `agg(sum("amount").alias("total_spent"))`
- `withColumn("generated_at", current_timestamp())`
- `withColumnRenamed("name", "client_name")`
- `orderBy(col("total_spent").desc())`

То есть смысл витрины очень простой:

- одна строка = один клиент;
- показатель = суммарные траты по всем заказам;
- дополнительное поле = время генерации набора.

### Шаг 5. Запись в `mart`

Запись выполняется через:

- `report_df.write.jdbc(..., table="mart.client_spend_report", mode="append")`

Именно режим `append` создает главное ограничение пайплайна:
каждый повторный запуск дописывает новый набор строк поверх старых.

## Схема движения данных

Полный поток practice 1 выглядит так:

1. Airflow создает и очищает `raw`.
2. Airflow создает `mart`.
3. Python-функция кладет в `raw` 100 клиентов и 500 заказов.
4. Airflow вызывает Spark через `SparkSubmitOperator`.
5. Spark читает `raw` по JDBC.
6. Spark считает `total_spent` по клиенту.
7. Spark пишет результат в `mart.client_spend_report`.

## Почему пайплайн сделан именно так

С точки зрения учебной задачи у дизайна есть понятный смысл:

- показать `SQLExecuteQueryOperator`;
- показать `PythonOperator` и `execute_values`;
- показать `SparkSubmitOperator`;
- показать чтение и запись PostgreSQL из Spark;
- дать минимальный пример `raw -> mart`.

Но это именно демонстрационный контур, а не архитектура, рассчитанная на долгую эксплуатацию.

## Главные ограничения и неоднозначности

### 1. `mart` не идемпотентен

`raw` пересобирается заново, а `mart` — нет.
В результате повторный запуск не дает «тот же отчет», а добавляет еще одну копию отчета.

### 2. Metadata Airflow и данные practice 1 живут в одной базе

Это удобно локально, но методологически смешивает служебный и бизнесовый слой.

### 3. Spark job hardcoded по подключению

JDBC URL и credentials зашиты прямо в `process_data.py`, а не переиспользуют Airflow connection.

### 4. Модель очень узкая

В practice 1 всего две raw-таблицы и одна mart-таблица.
Это удобно для входа, но не показывает более сложные DWH-паттерны.

### 5. Нет проверки качества данных

Пайплайн не валидирует:

- пустые значения;
- аномальные суммы;
- дубли;
- корректность итоговой витрины.

## Итог

`elt_pipeline` полезен как компактный пример связки Airflow + PostgreSQL + Spark.
Он отлично показывает orchestration и техническую интеграцию сервисов, но с точки зрения аналитической модели и идемпотентности остается сознательно учебным и упрощенным.

