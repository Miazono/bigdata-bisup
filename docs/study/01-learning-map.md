# Карта изучения и состав материалов

## Назначение

Этот документ задает структуру всего учебного комплекта и фиксирует логику, по которой он собран.
Его задача — держать материалы в одном маршруте изучения, а не превращать их в несвязанный набор заметок.

## Принцип деления

Материалы разделены на четыре типа:

- опорные документы — словарь и карта изучения;
- теория — технологии и подходы, которые нужны для понимания проекта;
- проектные разборы — фактическое устройство именно этого репозитория;
- script guides — быстрые схемы и аннотированные копии ключевых файлов.

## Блок 1. Опорные документы

Этот блок нужно читать первым, потому что он фиксирует термины, различия между двумя практиками и структуру всего комплекта.

Состав блока:

- `README.md`
- `00-glossary.md`
- `01-learning-map.md`

## Блок 2. Теория

Теоретический блок нужен для того, чтобы понимать не только последовательность действий в коде, но и смысл используемых подходов.
Он должен объяснять технологии, но не заменять собой разбор конкретного репозитория.

План блока:

- `theory/README.md`
- `theory/01-etl-elt-orchestration-and-dwh.md`
- `theory/02-analytics-data-modeling-and-layers.md`
- `theory/03-postgresql-sql-used-in-project.md`
- `theory/04-airflow-core-concepts.md`
- `theory/05-python-libraries-and-data-access.md`
- `theory/06-spark-and-pyspark.md`
- `theory/07-postgresql-as-analytical-store.md`
- `theory/08-grafana-in-analytics.md`
- `theory/09-docker-compose-networks-volumes-and-env.md`
- `theory/10-local-learning-stack.md`

Что должен закрыть этот блок:

- различие между ETL и ELT;
- роль orchestration и место Airflow;
- назначение слоев `raw`, `stg`, `dds`, `dm`, `mart`;
- моделирование измерений, фактов и витрин;
- SQL-конструкции PostgreSQL, реально используемые в проекте;
- доступ к данным из Python, `pandas`, `SQLAlchemy`, `psycopg2`, `PySpark`;
- роли Spark, PostgreSQL, Grafana и Docker Compose в локальном учебном контуре.

## Блок 3. Разбор проекта

Этот блок отвечает на вопрос, как устроен именно `airflow-dwh-proj`.
Здесь важно не додумывать систему, а опираться на реальные файлы, реальные связи контейнеров и фактическую логику DAG-ов.

План блока:

- `project/README.md`
- `project/01-repository-architecture.md`
- `project/02-shared-infrastructure.md`
- `project/03-practice-1-elt-pipeline.md`
- `project/04-practice-2-crm-report-daily.md`
- `project/05-sql-deep-dive.md`
- `project/06-python-dags-and-scripts.md`
- `project/07-airflow-connections-and-config.md`
- `project/08-pipeline-flows.md`
- `project/09-grafana-role.md`

Что должен закрыть этот блок:

- общую архитектуру репозитория и разделение на два пайплайна;
- структуру и назначение `airflow/docker-compose.yaml` и `spark/docker-compose.yaml`;
- связи между Airflow, Redis, PostgreSQL, Spark и Grafana;
- детальный разбор `elt_pipeline.py`, `crm_stats_daily.py`, `process_data.py` и SQL-файлов;
- назначение таблиц, ключей, слоев и паттернов загрузки;
- особенности текущей реализации, упрощения и спорные решения.

## Блок 4. Script guides

Этот блок не является основным способом изучения проекта.
Он нужен как вспомогательный слой для быстрого восстановления контекста по конкретному файлу.

План блока:

- `script-guides/README.md`
- `script-guides/maps/elt_pipeline.md`
- `script-guides/maps/crm_stats_daily.md`
- `script-guides/maps/process_data.md`
- `script-guides/maps/init_dwh.md`
- `script-guides/maps/airflow-docker-compose.md`
- `script-guides/maps/spark-docker-compose.md`
- `script-guides/annotated/elt_pipeline.py.md`
- `script-guides/annotated/crm_stats_daily.py.md`
- `script-guides/annotated/process_data.py.md`

Что должен закрыть этот блок:

- быстрое напоминание, за что отвечает каждый ключевой файл;
- список входов, выходов, зависимостей и основных шагов;
- построчное чтение самых важных Python-файлов без необходимости открывать код отдельно.

## Рекомендуемый порядок подготовки

1. Зафиксировать каркас, словарь и маршруты чтения.
2. Подготовить теоретический блок под фактический стек проекта.
3. Разобрать общую инфраструктуру и обе практики.
4. Детализировать SQL, Python и конфигурации.
5. Добавить script guides как вспомогательный быстрый слой.
6. Проверить, что термины, ссылки и названия сущностей совпадают во всех файлах.

## Рекомендуемый порядок изучения

1. `00-glossary.md`
2. `01-learning-map.md`
3. `theory/README.md`
4. Теоретические документы по архитектуре, моделированию, SQL и Airflow
5. Теория по Spark, PostgreSQL, Grafana и Docker Compose
6. `project/01-repository-architecture.md`
7. `project/02-shared-infrastructure.md`
8. `project/03-practice-1-elt-pipeline.md`
9. `project/04-practice-2-crm-report-daily.md`
10. `project/05-sql-deep-dive.md`
11. `project/06-python-dags-and-scripts.md`
12. `project/07-airflow-connections-and-config.md`
13. `project/08-pipeline-flows.md`
14. `project/09-grafana-role.md`
15. `script-guides/README.md`

## Что сознательно не входит в комплект

- блоки вида «что могут спросить на защите»;
- объяснение технологии только на примере текущего проекта;
- описание несуществующих DAG-ов, таблиц, схем или настроек как уже реализованных;
- подмена реального разбора общими абстрактными рассуждениями.

