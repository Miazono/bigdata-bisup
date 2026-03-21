# Теоретический блок

## Назначение

Этот раздел объясняет технологии и подходы, которые нужны для понимания проекта `airflow-dwh-proj`.
Он не должен подменять собой разбор репозитория, поэтому здесь акцент сделан на принципах, механизмах и типовых паттернах.

## Состав раздела

- [Архитектура ETL, ELT, orchestration и DWH](01-etl-elt-orchestration-and-dwh.md)
- [Моделирование данных для аналитики и слои данных](02-analytics-data-modeling-and-layers.md)
- [SQL и PostgreSQL, используемые в проекте](03-postgresql-sql-used-in-project.md)
- [Airflow: базовые сущности и механика выполнения](04-airflow-core-concepts.md)
- [Python-библиотеки и доступ к данным в DAG-ах и скриптах](05-python-libraries-and-data-access.md)
- [Spark и PySpark для учебного пайплайна](06-spark-and-pyspark.md)
- [PostgreSQL как учебное аналитическое хранилище](07-postgresql-as-analytical-store.md)
- [Grafana в аналитическом контуре](08-grafana-in-analytics.md)
- [Docker Compose, сети, volume и локальная инфраструктура](09-docker-compose-networks-volumes-and-env.md)
- [Локальный учебный контур: роли сервисов и границы ответственности](10-local-learning-stack.md)

## Что должен дать раздел

- понимание архитектурных различий между ETL и ELT;
- понимание слоистого устройства аналитических систем;
- базу по Airflow, Spark, PostgreSQL, Grafana и Docker Compose;
- понимание того, зачем в проекте одновременно нужны оркестратор, база, вычислительный движок и слой визуализации.

