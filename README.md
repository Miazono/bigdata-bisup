# Практика Big Data

Учебный проект по оркестрации ETL в Apache Airflow и трансформации данных в Apache Spark с загрузкой в DWH на PostgreSQL. 
Содержит две практики: базовый ETL-пайплайн с обработкой Spark-ом и витринную модель CRM-данных.

**Стек и компоненты**
- Apache Airflow
- Apache Spark
- PostgreSQL DWH
- Grafana

**Архитектура и структура**
- `airflow/` — DAG-и, SQL, init скрипты, `docker-compose.yaml`
- `spark/` — Spark cluster + job
- `dataset/` — sample CSV
- `data_generator.py` — генератор данных

Связи: Airflow управляет задачами, Spark выполняет трансформации и пишет в DWH, PostgreSQL хранит витрины.

**Практики**
1. **Практика 1 — DAG `elt_pipeline`**
   - Создание RAW и MART таблиц
   - Генерация мок-данных
   - Spark-трансформация
   - Запись витрины в `mart`
2. **Практика 2 — DAG `crm_report_daily`**
   - STG → DDS → DM
   - Загрузка CSV в staging
   - Построение фактов и витрин

Используемые схемы:
- Практика 1: `raw`, `mart`
- Практика 2: `stg`, `dds`, `dm`

**DAGS**
**Практика 1 — ETL и витрина трат клиентов (elt_pipeline)**
- Назначение: имитируем заказы клиентов и строит витрину трат по клиенту.
- Данные:
  - `raw.clients`: клиенты (100 записей) с датой регистрации.
  - `raw.orders`: заказы (500 записей), суммы и даты.
- Логика:
  - В Airflow генерируются клиенты и заказы (mock).
  - Spark читает `raw.*`, агрегирует `total_spent` по клиенту, добавляет `generated_at`.
  - Результат пишется в `mart.client_spend_report`.
- Итоговая витрина: рейтинг клиентов по суммарным тратам.

**Практика 2 — CRM/DWH витрины маркетинга (crm_report_daily)**
- Назначение: имитируем агентскую отчетность по клиентам, кампаниям и состоянию сайтов.
- Входные файлы (CSV):
  - `clients.csv`, `campaigns.csv` — справочники.
  - `ad_stats_{date}.csv` — ежедневная статистика рекламы.
  - `site_monitoring_{date}.csv` — ежедневный мониторинг сайтов.
- Слои:
  - STG: сырые CSV (все поля как `TEXT`).
  - DDS: справочники клиентов/кампаний + факты рекламы и мониторинга.
  - DM: витрины KPI и агрегации.
- Логика витрин:
  - `dm.client_kpi`: месячные расходы, конверсии и CPL по клиентам.
  - `dm.platform_stats`: дневные CTR/CPC и клики по платформам.
  - `dm.site_reliability`: дневная надежность сайтов и статус `OK/PROBLEM`.
  - `dm.agency_finance`: месячная выручка и комиссия по менеджерам.

**Быстрый старт**
1. Требования: установлен `docker` и `docker compose`.
2. Запуск Airflow:
```bash
cd airflow
docker compose up -d --build
```
3. Запуск Spark:
```bash
cd ../spark
docker compose up -d --build
```
4. Доступы к UI:
- Airflow: `http://localhost:8080`
- Spark Master UI: `http://localhost:5080`
- Spark Worker UI: `http://localhost:8081`
- Grafana: `http://localhost:3000`

**Входные данные**
- DAG `crm_report_daily` читает CSV из `airflow/dags/input/`.
- В репозитории хранится только sample в `dataset/`.
- Перед запуском скопируй sample в `airflow/dags/input/`:
```bash
cp -r ../dataset/* ./airflow/dags/input/
```

**Важные настройки**
- Airflow connections задаются через env:
  - `AIRFLOW_CONN_DWH_POSTGRES`
  - `AIRFLOW_CONN_POSTGRES_MEDIANATION`
  - `AIRFLOW_CONN_SPARK_DEFAULT`
- Spark использует внешнюю сеть `shared-data-network`.
  Если сеть еще не создана:
```bash
docker network create shared-data-network
```

**FAQ / Troubleshooting**
- DAG не видит CSV: проверь `airflow/dags/input/`.
- Spark не подключается: проверь сеть `shared-data-network`.
- Логи не отображаются: проверь health контейнеров `airflow-worker`, `airflow-triggerer`, `airflow-dag-processor`.

**Проверка (manual)**
- Поднять `airflow` и `spark`.
- Запустить `elt_pipeline` и проверить витрину `mart`.
- Скопировать sample CSV и запустить `crm_report_daily`.
