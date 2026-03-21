# Конфигурация Airflow, connections, volumes и сети

## Какие файлы отвечают за конфигурацию

Для понимания configuration-контура важно смотреть на:

- `airflow/docker-compose.yaml`;
- `airflow/Dockerfile`;
- `airflow/requirements.txt`;
- `airflow/init.sh`;
- `airflow/dags/common/constants.py`;
- `airflow/dags/elt_pipeline.py`;
- `airflow/dags/crm_stats_daily.py`.

Именно вместе они объясняют:

- как Airflow собирается;
- какие providers доступны;
- какие connections задаются через env;
- как Airflow видит Spark apps;
- как DAG-и используют эти подключения.

## Как собирается образ Airflow

`airflow/Dockerfile` строится от `apache/airflow:3.1.0`.
Поверх него добавляются:

- `openjdk-17-jdk`;
- `curl`;
- `tar`.

Затем выставляется `JAVA_HOME`, а под пользователем `airflow` ставятся зависимости из `requirements.txt`.

### Почему Airflow нужен Java

Не потому, что DAG-и написаны на Java.
Java нужна для запуска Spark submit и общей совместимости со Spark-инструментами из контейнера Airflow.

## Что лежит в `requirements.txt`

Файл содержит:

- `apache-airflow-providers-apache-spark`;
- `apache-airflow-providers-common-sql`;
- `apache-airflow-providers-postgres`;
- `pyspark==3.5.1`.

Это почти прямой список технологий, которые реально используются в DAG-ах.

## Connection env через `AIRFLOW_CONN_*`

В `airflow/docker-compose.yaml` заданы три ключевых connection:

### `AIRFLOW_CONN_DWH_POSTGRES`

Значение:

- `postgresql://airflow:airflow@postgres:5432/airflow`

Соответствующий `conn_id` в Airflow:

- `dwh_postgres`

Именно его использует practice 1 через `DBConnections.DWH`.

### `AIRFLOW_CONN_POSTGRES_MEDIANATION`

Значение:

- `postgresql://medianation:medianation@postgres-dwh:5432/medianation_dwh`

Соответствующий `conn_id`:

- `postgres_medianation`

Именно его использует practice 2 через `Config.DB_CONN_ID`.

### `AIRFLOW_CONN_SPARK_DEFAULT`

Значение:

- `spark://master:7077`

Соответствующий `conn_id`:

- `spark_default`

Именно его использует `SparkSubmitOperator` в practice 1.

## Как DAG-и используют connections

### Practice 1

Через `common/constants.py`:

- `dwh_postgres`;
- `spark_default`.

### Practice 2

Через строку в `Config`:

- `postgres_medianation`.

### Что важно заметить

Внутри Spark script `process_data.py` connection Airflow уже не используется.
Там JDBC URL и credentials захардкожены:

- `jdbc:postgresql://postgres:5432/airflow`
- `user=airflow`
- `password=airflow`

То есть конфигурация practice 1 разделена между:

- Airflow connection;
- Spark script;
- Docker network hostnames.

Это работает, но не является единой централизованной схемой конфигурации.

## Другие важные env-настройки Airflow

В compose также явно заданы:

- `AIRFLOW__CORE__EXECUTOR: CeleryExecutor`;
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`;
- `AIRFLOW__CELERY__RESULT_BACKEND`;
- `AIRFLOW__CELERY__BROKER_URL`;
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`;
- `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK`;
- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`;
- `AIRFLOW__CORE__LOAD_EXAMPLES`.

Это показывает, что Airflow работает как полноценный локальный cluster-mode стенд, а не как минимальный single-container вариант.

## Volumes

### Стандартные volumes Airflow

Монтируются:

- `dags`;
- `logs`;
- `config`;
- `plugins`.

Это позволяет:

- редактировать DAG-и с хоста;
- сохранять логи между перезапусками;
- управлять конфигом без пересборки контейнера.

### Shared volume со Spark

Дополнительно монтируется:

- `../spark/apps -> /opt/spark/work-dir`

Это один из ключевых элементов всей архитектуры.
Благодаря ему Airflow worker видит:

- `process_data.py`;
- `postgresql-42.7.3.jar`.

Именно поэтому `SparkSubmitOperator` может ссылаться на эти файлы как на локальные пути внутри Airflow-контейнера.

## Сеть `shared-data-network`

В `airflow/docker-compose.yaml` сеть `default` получает фиксированное имя:

- `shared-data-network`

В `spark/docker-compose.yaml` та же сеть подключается как внешняя.

Это нужно, чтобы:

- Airflow видел Spark master по имени `master`;
- Spark job видел PostgreSQL по имени `postgres`;
- отдельные compose-проекты находились в одном DNS-пространстве Docker.

## Почему Spark вынесен в отдельный compose

Есть несколько причин:

- архитектурно Spark отделен от Airflow как compute layer;
- проще поднимать и останавливать кластер отдельно;
- легче показать границу между orchestration и вычислениями;
- проще объяснить, что Spark не встроен в Airflow.

Цена такого решения тоже есть:

- нужна общая внешняя сеть;
- shared volume становится обязательным;
- больше точек, где пути и hostname должны совпадать.

## `airflow/init.sh`

Скрипт:

1. создает `.env`, если его нет;
2. записывает туда `AIRFLOW_UID` и `AIRFLOW_PROJ_DIR`;
3. скачивает JDBC driver в `../spark/apps/postgresql-42.7.3.jar`.

### Важный нюанс

Проверка наличия файла сделана по пути:

- `apps/postgresql-42.7.3.jar`

А скачивание идет по пути:

- `../spark/apps/postgresql-42.7.3.jar`

То есть логика «не скачивать лишний раз» описана не полностью аккуратно.

## Что в конфигурации проекта выглядит спорно или упрощенно

### 1. `AIRFLOW_CONN_DWH_POSTGRES` по имени вводит в заблуждение

Название намекает на DWH, но connection ведет в `postgres`, где живут metadata Airflow и данные practice 1.

### 2. Конфигурация practice 1 не полностью централизована

Airflow connection и Spark JDBC настройки живут в разных местах.

### 3. Practice 2 не переиспользует общий модуль constants

Это не критично, но показывает, что репозиторий эволюционировал не как единая библиотека, а как набор учебных практик.

### 4. Shared volume обязателен

Без монтирования `spark/apps` из Airflow в practice 1 не будет виден ни job, ни JDBC jar.

## Итог

Конфигурация Airflow в этом проекте хорошо показывает реальные механизмы локального orchestration-контура:

- connections через env;
- разделение на scheduler, worker, triggerer и dag-processor;
- связь с Redis и PostgreSQL;
- интеграцию со Spark через shared network и shared volume.

При этом именно на уровне конфигурации особенно хорошо видно, что репозиторий учебный:
он рабочий, но не пытается скрыть упрощения и локальные компромиссы.

