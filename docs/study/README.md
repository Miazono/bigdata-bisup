# Учебный комплект по проекту `airflow-dwh-proj`

## Навигация

### Базовые опорные документы

- [Словарь терминов и соглашений](00-glossary.md)
- [Карта изучения и состав материалов](01-learning-map.md)

### Теория

- [Обзор теоретического блока](theory/README.md)
- [Архитектура ETL, ELT, orchestration и DWH](theory/01-etl-elt-orchestration-and-dwh.md)
- [Моделирование данных для аналитики и слои данных](theory/02-analytics-data-modeling-and-layers.md)
- [SQL и PostgreSQL, используемые в проекте](theory/03-postgresql-sql-used-in-project.md)
- [Airflow: базовые сущности и механика выполнения](theory/04-airflow-core-concepts.md)
- [Python-библиотеки и доступ к данным в DAG-ах и скриптах](theory/05-python-libraries-and-data-access.md)
- [Spark и PySpark для учебного пайплайна](theory/06-spark-and-pyspark.md)
- [PostgreSQL как учебное аналитическое хранилище](theory/07-postgresql-as-analytical-store.md)
- [Grafana в аналитическом контуре](theory/08-grafana-in-analytics.md)
- [Docker Compose, сети, volume и локальная инфраструктура](theory/09-docker-compose-networks-volumes-and-env.md)
- [Локальный учебный контур: роли сервисов и границы ответственности](theory/10-local-learning-stack.md)

### Разбор проекта

- [Общая архитектура репозитория](project/01-repository-architecture.md)
- [Общая инфраструктура и контейнерный контур](project/02-shared-infrastructure.md)
- [Практика 1: пайплайн `elt_pipeline`](project/03-practice-1-elt-pipeline.md)
- [Практика 2: пайплайн `crm_report_daily`](project/04-practice-2-crm-report-daily.md)
- [Подробный разбор SQL в проекте](project/05-sql-deep-dive.md)
- [Подробный разбор Python, DAG-ов и скриптов](project/06-python-dags-and-scripts.md)
- [Конфигурация Airflow, connections, volumes и сети](project/07-airflow-connections-and-config.md)
- [Схемы движения данных и точки интеграции](project/08-pipeline-flows.md)
- [Роль Grafana: текущее и целевое состояние](project/09-grafana-role.md)

### Путеводители по скриптам

- [Обзор формата script guides](script-guides/README.md)
- [Карта `elt_pipeline.py`](script-guides/maps/elt_pipeline.md)
- [Карта `crm_stats_daily.py`](script-guides/maps/crm_stats_daily.md)
- [Карта `process_data.py`](script-guides/maps/process_data.md)
- [Карта `init_dwh.sql`](script-guides/maps/init_dwh.md)
- [Карта `docker-compose.yaml` для Airflow](script-guides/maps/airflow-docker-compose.md)
- [Карта `docker-compose.yaml` для Spark](script-guides/maps/spark-docker-compose.md)
- [Аннотированная копия `elt_pipeline.py`](script-guides/annotated/elt_pipeline.py.md)
- [Аннотированная копия `crm_stats_daily.py`](script-guides/annotated/crm_stats_daily.py.md)
- [Аннотированная копия `process_data.py`](script-guides/annotated/process_data.py.md)

## Назначение

Этот раздел нужен не для краткого знакомства с проектом, а для глубокого разбора того, как устроен репозиторий `airflow-dwh-proj` и как работают его технологии.
Цель комплекта двойная:

- изучить теорию, которая реально нужна для понимания проекта;
- понять именно текущую реализацию: контейнеры, базы, DAG-и, SQL, Spark job, загрузки CSV и подготовку витрин.

## На что опираются материалы

Факты о проекте должны опираться прежде всего на реальные файлы репозитория:

- `README.md`;
- `airflow/docker-compose.yaml`;
- `airflow/Dockerfile`;
- `airflow/requirements.txt`;
- `airflow/init.sh`;
- `airflow/dags/elt_pipeline.py`;
- `airflow/dags/crm_stats_daily.py`;
- `airflow/dags/sql/create_raw_tables.sql`;
- `airflow/dags/sql/create_mart_tables.sql`;
- `airflow/scripts/init_db.sql`;
- `airflow/scripts/init_dwh.sql`;
- `spark/docker-compose.yaml`;
- `spark/Dockerfile`;
- `spark/apps/process_data.py`;
- `dataset/`;
- `data_generator.py`.

Если учебный текст расходится с кодом, доверять нужно коду и конфигурации.

## Логика деления материалов

Комплект разделен на четыре группы:

- `00-glossary.md` и `01-learning-map.md` фиксируют терминологию и порядок изучения;
- `theory/` объясняет технологии и подходы без подмены проектного разбора;
- `project/` разбирает фактическую реализацию этого репозитория;
- `script-guides/` дает быстрые шпаргалки и аннотированные копии ключевых файлов.

## Принципы оформления

- Термины `raw`, `mart`, `stg`, `dds`, `dm`, `DWH`, `витрина`, `факт`, `измерение`, `идемпотентность` и `upsert` должны использоваться последовательно.
- Теория должна быть релевантна стеку проекта, но не должна объяснять материал на примере текущего репозитория.
- Проектные разборы должны честно отмечать упрощения, спорные решения и неполные части реализации.
- `mart` и `dm` не считаются взаимозаменяемыми понятиями: в проекте это разные контуры и разные уровни моделирования.

## Рекомендуемый порядок чтения

1. `00-glossary.md`
2. `01-learning-map.md`
3. `theory/README.md`
4. Документы из `theory/`
5. `project/01-repository-architecture.md`
6. `project/02-shared-infrastructure.md`
7. `project/03-practice-1-elt-pipeline.md`
8. `project/04-practice-2-crm-report-daily.md`
9. `project/05-sql-deep-dive.md`
10. `project/06-python-dags-and-scripts.md`
11. `project/07-airflow-connections-and-config.md`
12. `project/08-pipeline-flows.md`
13. `project/09-grafana-role.md`
14. `script-guides/README.md`

