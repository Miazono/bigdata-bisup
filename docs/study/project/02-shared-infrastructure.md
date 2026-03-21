# Общая инфраструктура и контейнерный контур

## Что именно образует общий контур

Общий контур проекта складывается из двух compose-частей:

- `airflow/docker-compose.yaml`;
- `spark/docker-compose.yaml`.

Первая часть поднимает orchestration, базы, Redis и Grafana.
Вторая поднимает Spark master и worker.

Они не независимы друг от друга.
Их связывают:

- общая сеть `shared-data-network`;
- shared volume с `spark/apps`;
- согласованные hostnames в connection и JDBC-путях.

## Разбор `airflow/docker-compose.yaml`

### Общий шаблон `x-airflow-common`

Файл построен вокруг YAML-anchor `x-airflow-common`.
В нем централизованы:

- сборка кастомного образа Airflow через `build: .`;
- общие переменные окружения;
- volumes;
- пользователь контейнера;
- зависимости от `redis` и `postgres`.

Это типичный прием для Compose-файлов Airflow: не дублировать одинаковую конфигурацию в `scheduler`, `worker`, `triggerer` и других сервисах.

### Что задается в environment

Ключевые настройки:

- `AIRFLOW__CORE__EXECUTOR: CeleryExecutor`;
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` на сервис `postgres`;
- `AIRFLOW__CELERY__RESULT_BACKEND` тоже через PostgreSQL;
- `AIRFLOW__CELERY__BROKER_URL` через Redis;
- `AIRFLOW__CORE__LOAD_EXAMPLES: 'true'`;
- `AIRFLOW_CONN_DWH_POSTGRES`;
- `AIRFLOW_CONN_POSTGRES_MEDIANATION`;
- `AIRFLOW_CONN_SPARK_DEFAULT`.

Это сразу показывает, что Airflow в этом проекте настроен не как standalone, а как распределенный локальный кластер на Celery.

### Volumes в Airflow-контуре

В Airflow монтируются:

- `./dags -> /opt/airflow/dags`;
- `./logs -> /opt/airflow/logs`;
- `./config -> /opt/airflow/config`;
- `./plugins -> /opt/airflow/plugins`;
- `../spark/apps -> /opt/spark/work-dir`.

Последний mount особенно важен.
Он делает Spark application и JDBC jar видимыми из Airflow-контейнеров.
Именно поэтому `SparkSubmitOperator` может запускать `/opt/spark/work-dir/process_data.py`.

## Сервисы Airflow-кластера

### `postgres`

Сервис `postgres` поднимается на `postgres:13`.
Он выполняет сразу две роли:

- metadata DB Airflow;
- storage для practice 1, где лежат `raw` и `mart`.

Это рабочее учебное решение, но архитектурно неидеальное.
В production metadata оркестратора и бизнес-данные обычно разводят.

### `redis`

`redis` выступает как Celery broker.
Он нужен, чтобы `scheduler` мог ставить задачи в очередь, а `worker` — забирать их на выполнение.

### `airflow-apiserver`

Предоставляет Airflow UI и API на `8080`.

### `airflow-scheduler`

Отвечает за создание DAG runs и планирование task instance.

### `airflow-dag-processor`

Отдельно разбирает DAG-файлы как Python-код.

### `airflow-worker`

Исполняет задачи через Celery.
Именно здесь реально запускаются:

- `PythonOperator`;
- `SQLExecuteQueryOperator`;
- `SparkSubmitOperator`.

### `airflow-triggerer`

Поддерживает deferrable-механику Airflow.
В текущих DAG-ах он не главный герой, но как сервис присутствует в полном кластере.

### `airflow-init`

Сервис инициализации выполняет:

- миграции базы Airflow;
- создание пользователя UI;
- подготовку директорий и прав;
- базовую проверку ресурсов.

### `airflow-cli` и `flower`

`airflow-cli` оставлен как debug-профиль.
`flower` доступен как опциональный профиль мониторинга Celery.

## Отдельные сервисы для данных и визуализации

### `postgres-dwh`

Это отдельный PostgreSQL-контур для practice 2.
Он:

- использует собственный volume `dwh-db-volume`;
- инициализируется через `./scripts/init_dwh.sql`;
- слушает наружу порт `5434`, а внутри сети доступен как `postgres-dwh:5432`.

Именно здесь лежат схемы `stg`, `dds` и `dm`.

### `grafana`

Grafana поднимается как `grafana/grafana:latest`.
В compose-файле ей заданы только:

- порт `3000`;
- логин и пароль `admin/admin`;
- зависимость от `postgres-dwh`.

Важно, что в репозитории нет:

- provisioning datasource;
- provisioning dashboards;
- volume для постоянного хранения конфигурации Grafana;
- SQL-запросов или JSON dashboard-файлов.

То есть Grafana пока присутствует как поднятый сервис, а не как полностью описанный в коде BI-контур.

## Разбор `airflow/Dockerfile`

Образ Airflow строится от `apache/airflow:3.1.0`.
Поверх базового образа добавляются:

- `openjdk-17-jdk`;
- `curl`;
- `tar`.

Установка Java нужна потому, что Airflow-контейнер должен уметь запускать Spark submit.
Без Java работа со Spark из Airflow была бы неполной.

После этого под пользователем `airflow` устанавливаются пакеты из `requirements.txt`.

## Разбор `airflow/requirements.txt`

В requirements лежат:

- `apache-airflow-providers-apache-spark`;
- `apache-airflow-providers-common-sql`;
- `apache-airflow-providers-postgres`;
- `pyspark==3.5.1`.

Этот набор хорошо отражает реальные потребности проекта:

- Spark operator и hook;
- SQL operator;
- PostgreSQL hook;
- локальный доступ к PySpark API в образе Airflow.

## Разбор `airflow/init.sh`

Скрипт делает две вещи:

1. создает `.env` с `AIRFLOW_UID` и `AIRFLOW_PROJ_DIR`;
2. скачивает PostgreSQL JDBC driver в `../spark/apps/postgresql-42.7.3.jar`.

Но в скрипте есть важная мелкая несогласованность:

- проверка идет по пути `apps/postgresql-42.7.3.jar` относительно каталога `airflow/`;
- скачивание выполняется в `../spark/apps/postgresql-42.7.3.jar`.

Из-за этого проверка и целевой путь не совпадают.
Практически это означает, что логика «скачивать только если файла нет» реализована неаккуратно.

## Разбор `spark/docker-compose.yaml`

Spark вынесен в отдельный compose-файл.
Внутри него два сервиса:

- `master`;
- `worker`.

`master` публикует:

- `5080` для web UI;
- `7077` для Spark master;
- `4040` для driver UI.

`worker` подключается к `spark://master:7077`, поднимает web UI на `8081` и ограничивается:

- `--cores 1`;
- `--memory 2G`.

Это локальный минимальный Spark cluster, пригодный для учебной демонстрации.

## Разбор `spark/Dockerfile`

Образ строится от `apache/spark:3.5.1`.
Внутрь добавляется PostgreSQL JDBC driver в `/opt/spark/jars/postgresql-42.7.3.jar`.

Это значит, что Spark image сам по себе готов к JDBC-работе с PostgreSQL.
Но проект дополнительно использует jar и в shared volume `spark/apps`, потому что `SparkSubmitOperator` передает его явным параметром `jars=...`.

## Общая сеть и межсервисные связи

### `shared-data-network`

В `airflow/docker-compose.yaml` сеть `default` создается с фиксированным именем `shared-data-network`.
В `spark/docker-compose.yaml` эта же сеть подключается как `external: true`.

Именно благодаря этому:

- Airflow видит Spark master по имени `master`;
- Spark job видит PostgreSQL по имени `postgres`;
- сервисы из разных compose-файлов оказываются в одном внутреннем DNS-пространстве.

### Кто к кому подключается

- Airflow `scheduler` и `worker` подключаются к `postgres` и `redis`.
- Practice 1 из Airflow подключается к `postgres`.
- Airflow через `SparkSubmitOperator` подключается к Spark master `master:7077`.
- Spark job из practice 1 читает и пишет в `postgres`.
- Practice 2 из Airflow подключается к `postgres-dwh`.
- Grafana должна подключаться к `postgres-dwh`, но это не зафиксировано в коде provisioning.

## Почему Spark вынесен в отдельный compose

Такое решение здесь оправдано учебно и организационно:

- Spark явно отделен от Airflow как вычислительный контур;
- Airflow не раздувается дополнительными сервисами кластера внутри одного compose;
- легче показать границу между orchestration и compute;
- можно отдельно поднимать и останавливать Spark-контур.

Но у решения есть и цена:

- нужна общая внешняя сеть;
- появляются дополнительные точки согласования путей;
- нужно синхронизировать shared volume с приложением и jar-файлом.

## Что важно зафиксировать честно

- practice 1 хранит данные в той же базе, где живет Airflow metadata;
- Grafana как сервис поднята, но как кодом описанный BI-контур пока не завершена;
- `init.sh` содержит несовпадение пути проверки и пути скачивания JDBC driver;
- Spark split по отдельному compose делает архитектуру наглядной, но чуть более хрупкой для локального запуска.

