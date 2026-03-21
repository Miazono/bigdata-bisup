# Подробный разбор Python, DAG-ов и скриптов

## Какие Python-файлы здесь действительно важны

Для понимания проекта ключевыми являются:

- `airflow/dags/common/constants.py`;
- `airflow/dags/elt_pipeline.py`;
- `airflow/dags/crm_stats_daily.py`;
- `spark/apps/process_data.py`;
- `data_generator.py`.

Именно они задают:

- идентификаторы connections и путей;
- orchestration двух практик;
- Spark-трансформацию practice 1;
- генерацию sample-данных practice 2.

## `airflow/dags/common/constants.py`

Файл очень короткий, но важный.
Он содержит три класса:

- `DBConnections`;
- `SparkConnections`;
- `Paths`.

### Что здесь зафиксировано

- `DBConnections.DWH = "dwh_postgres"`
- `DBConnections.META = "postgres_default"`
- `SparkConnections.DEFAULT = "spark_default"`
- `Paths.SPARK_APPS = "/opt/spark/work-dir"`
- `Paths.SPARK_JARS = "/opt/spark/work-dir"`

### Почему это полезно

Practice 1 не зашивает connection id и пути прямо в тело операторов.
Это делает DAG чуть аккуратнее.

### Но есть и ограничение

Practice 2 этим модулем не пользуется и держит свои значения в собственном `Config`.
То есть единый конфигурационный слой на весь репозиторий не сформирован.

## `airflow/dags/elt_pipeline.py`

## Импорты и константы

В начале файла импортируются:

- `DAG`;
- `SQLExecuteQueryOperator`;
- `PythonOperator`;
- `SparkSubmitHook`;
- `SparkSubmitOperator`;
- `PostgresHook`;
- `datetime`;
- `random`;
- `sys`.

`sys` при этом дальше не используется.
Это мелкий след учебной эволюции файла.

Затем задаются:

- `DB_CONN_ID = DBConnections.DWH`
- `SPARK_CONN_ID = SparkConnections.DEFAULT`

## `default_args`

В словаре:

- `owner = 'airflow'`
- `start_date = datetime(2024, 1, 1)`
- `catchup = False`

Параметр `catchup` здесь указан в `default_args`, а не прямо в конструкторе DAG.
С точки зрения читаемости это менее явно, чем явная передача `catchup` в `with DAG(...)`.

## Monkey patch `SparkSubmitHook`

Это самый нестандартный кусок файла.

### Как он устроен

1. Сохраняется оригинальный метод:
   - `_original_resolve_connection = SparkSubmitHook._resolve_connection`
2. Определяется новая функция:
   - `_resolve_connection_with_spark_master(self)`
3. Она получает исходные `conn_data`.
4. Если `master` уже корректный или отсутствует, поведение почти не меняется.
5. Если есть `conn.host`, она формирует:
   - `spark://{conn.host}:{conn.port}`
6. Затем происходит подмена метода класса.

### Что это значит по сути

DAG вмешивается во внутреннюю логику Spark provider, чтобы стабильно собрать корректный master URL из connection.
Для учебного стенда это приемлемо, но в production такой патч всегда нужно документировать отдельно, потому что он меняет стандартное поведение библиотеки.

## `generate_mock_data()`

Эта функция — основной Python-шаг practice 1.

### Что происходит по этапам

1. Создается `PostgresHook(postgres_conn_id=DB_CONN_ID)`.
2. Берется connection через `get_conn()`.
3. Открывается cursor.
4. Собирается список клиентов в Python.
5. Внутри функции локально импортируется:
   - `from psycopg2.extras import execute_values`
6. Через `execute_values` клиенты массово вставляются в `raw.clients`.
7. Выполняется `SELECT id FROM raw.clients`.
8. По этим `id` собирается список заказов.
9. Заказы массово вставляются в `raw.orders`.
10. При успехе делается `commit`, при ошибке — `rollback`.

### Что важно по синтаксису

- список клиентов и заказов формируется как список tuple;
- `execute_values(cursor, sql, values)` — быстрый способ вставить пачку строк;
- `try/except/finally` нужен для ручного контроля транзакции и закрытия ресурсов;
- `random.choice` и `random.uniform` используются для генерации значений.

### Почему это полезно для изучения

Здесь в одном месте видны:

- hook;
- raw psycopg2 connection;
- cursor;
- массовая вставка;
- ручная транзакция.

## Определение DAG и operator-ы practice 1

### `create_tables`

Это `SQLExecuteQueryOperator`, который выполняет файл `sql/create_raw_tables.sql`.
Параметр `split_statements=True` важен, потому что SQL-файл содержит несколько команд, разделенных `;`.

### `create_marts`

Почти то же самое, но для `sql/create_mart_tables.sql`.

### `generate_data`

Это `PythonOperator` с `python_callable=generate_mock_data`.

### `process_spark`

Это `SparkSubmitOperator`.
Именно он связывает Airflow и Spark.

Параметры оператора важны сами по себе:

- `application`;
- `jars`;
- ресурсы executors и driver;
- `deploy_mode='client'`;
- `verbose=True`.

## `airflow/dags/crm_stats_daily.py`

Это самый насыщенный Python-файл репозитория.

## Импорты

Используются:

- `os`;
- `pandas as pd`;
- `datetime`, `timedelta`;
- `logging`;
- `DAG`;
- `PythonOperator`;
- `PostgresHook`.

Здесь уже нет отдельного SQL-оператора и Spark-оператора.
Почти вся логика реализована через Python-функции + SQL внутри них.

## Класс `Config`

`Config` — это набор class attributes:

- `DAG_ID`;
- `DB_CONN_ID`;
- `INPUT_PATH`;
- имена схем;
- имена таблиц всех слоев.

Такой прием полезен, потому что:

- f-string вида `f"{SCHEMA_DDS}.clients"` собирается один раз;
- имена таблиц не размазаны по файлу.

Но есть и методическое ограничение:

- конфигурация не вынесена в общий модуль репозитория;
- она существует только внутри этого DAG-файла.

## Вспомогательные функции

### `get_postgres_engine()`

Создает `PostgresHook` и возвращает `get_sqlalchemy_engine()`.
Это мост между Airflow connection и `pandas.to_sql`.

### `run_sql()`

Минимальная обертка над `hook.run`.
Параметр `parameters=params` позволяет подставлять в SQL дату из Airflow.

### `read_csv_safe()`

Использует `os.path.join`, `os.path.exists` и `pd.read_csv`.
Если файла нет, сразу бросает исключение.

### `truncate_stg_tables()`

Формирует список таблиц, объединяет их через `', '.join(tables)` и выполняет один `TRUNCATE`.
Здесь хорошо видно, как Python собирает SQL динамически, но в рамках очень контролируемого списка таблиц.

## `load_stg_csv()`

Это универсальный loader для всех CSV.

### Что делает функция

1. Берет `execution_date = kwargs.get('ds')`.
2. Если имя файла содержит `{date}`, форматирует строку.
3. Читает CSV.
4. Делает `df = df.astype(str)`.
5. Получает engine.
6. Разбивает имя таблицы на `schema, table = table_name.split('.')`.
7. Пишет через `df.to_sql(..., if_exists='append', index=False)`.

### Что важно по синтаксису

- `**kwargs` позволяет получить Airflow context;
- `split('.')` используется для разбиения `schema.table`;
- `to_sql` работает через SQLAlchemy engine, а не через raw cursor.

## `load_dds_clients()` и `load_dds_campaigns()`

Обе функции построены одинаково:

- SQL-деактивация отсутствующих записей;
- SQL-upsert актуальных записей.

Они полезны тем, что показывают переход от `pandas`-стадии к полностью SQL-стадии.

### Особенность `load_dds_campaigns()`

Используется `TO_DATE(start_date, 'YYYY-MM-DD')`.
То есть типизация даты выполняется уже внутри SQL, а не на этапе pandas.

## `load_dds_facts()`

Это вспомогательная функция-обертка над общим паттерном:

1. взять `ds`;
2. выполнить `DELETE`;
3. выполнить `INSERT`.

Она уменьшает дублирование между рекламным и мониторинговым фактами.

## `load_fact_advertising()` и `load_fact_site()`

Эти функции уже почти полностью состоят из SQL-строк.
Python здесь нужен в основном для:

- хранения шаблона запроса;
- передачи параметров;
- вызова общей обертки.

Это хороший пример того, как DAG-файл может стать контейнером для SQL-логики, не превращаясь полностью в pandas-скрипт.

## `build_mart_daily()` и `build_mart_monthly()`

Обе функции — тонкие обертки:

- дневная просто выполняет один SQL с параметром даты;
- месячная выполняет сначала `DELETE`, затем `INSERT`.

С точки зрения имен они звучат как «строители витрин», но фактически являются короткими хелперами запуска SQL.

## `calc_dm_*`

В файле четыре витринные функции:

- `calc_dm_platform`
- `calc_dm_reliability`
- `calc_dm_kpi`
- `calc_dm_finance`

Различие между ними методически важно:

- две дневные витрины обновляются через `ON CONFLICT`;
- две месячные пересобираются через `DELETE + INSERT`.

## DAG definition practice 2

В `with DAG(...) as dag:` задаются:

- `Config.DAG_ID`;
- `default_args`;
- `catchup=False`;
- `max_active_runs=1`.

`schedule` явно не указан.
Это значит, что файл не описывает собственный регулярный cron-график и акцент в учебном проекте сделан на управляемый запуск по нужной дате.

## Зависимости practice 2

Граф зависимостей собран через оператор `>>`.
Он хорошо показывает, как из отдельных Python-функций складывается полноценный pipeline.

Особенно полезно замечать, что:

- сначала очищается `stg`;
- потом идет приемка файлов;
- потом справочники;
- затем факты;
- только после этого витрины.

## `spark/apps/process_data.py`

Это Spark job practice 1.

## Импорты

Используются:

- `SparkSession`;
- `sum as _sum`;
- `col`;
- `current_timestamp`.

Переименование `sum` в `_sum` — стандартный прием, чтобы не затереть встроенную Python-функцию `sum`.

## Функция `main()`

Скрипт полностью завернут в `main()`, а в конце вызывается через:

```python
if __name__ == "__main__":
    main()
```

Это нормальная Python-практика: код можно импортировать без немедленного исполнения.

## Шаги внутри `main()`

1. Создать `SparkSession`.
2. Задать `jdbc_url`.
3. Собрать `connection_properties`.
4. Прочитать `raw.clients`.
5. Прочитать `raw.orders`.
6. Выполнить `join`.
7. Выполнить `groupBy` и `agg`.
8. Добавить `generated_at`.
9. Переименовать `name` в `client_name`.
10. Отсортировать по `total_spent`.
11. Записать в `mart.client_spend_report`.
12. Остановить Spark.

### Что здесь полезно для изучения синтаксиса

- цепочки методов DataFrame;
- `alias` для именования агрегатов;
- `withColumn` для добавления поля;
- `withColumnRenamed` для переименования колонки;
- `write.jdbc(..., mode="append")` для записи результата.

## `data_generator.py`

Этот файл не участвует в DAG напрямую, но очень полезен для понимания входных данных practice 2.

## `get_master_data()`

Функция:

- проверяет, существуют ли `clients.csv` и `campaigns.csv`;
- если нет, генерирует их;
- если да, читает существующие.

Здесь используются:

- `os.path.exists`;
- `pd.read_csv`;
- `pd.DataFrame`;
- `to_csv`;
- `Faker('ru_RU')`;
- `random.choice`.

## `PLATFORM_TYPES`

Внутри генератора кампаний описан словарь допустимых типов кампаний по платформам:

- `Yandex`
- `Google`
- `VK`
- `Telegram`

Это полезная деталь, потому что показывает, что `platform` и `type` генерируются согласованно, а не случайно независимо друг от друга.

## `generate_daily_facts()`

Функция:

- берет `date_str`;
- переводит ее в `date`;
- делает `random.seed(process_date.toordinal())`;
- генерирует рекламный факт не по всем кампаниям, а по случайной активной доле;
- генерирует мониторинг по каждому клиенту;
- пишет дневные CSV.

### Почему `seed` важен

Один и тот же день воспроизводим.
Это очень полезно для учебного пайплайна: можно пересобрать sample-данные и получить одинаковый результат для одной даты.

## Итог

Python-код в репозитории показывает три разных стиля работы:

- компактный DAG + отдельный Spark job в practice 1;
- один насыщенный Python DAG с `pandas`, SQL и `PostgresHook` в practice 2;
- отдельный генератор sample-данных.

Именно эта комбинация делает репозиторий хорошим учебным материалом не только по SQL и Airflow, но и по стилям организации Python-кода в аналитическом проекте.

