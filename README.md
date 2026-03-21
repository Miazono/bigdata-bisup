# Практика Big Data

Учебный проект по оркестрации DWH-пайплайна в Apache Airflow с загрузкой данных в PostgreSQL.
В ветке `p2_demo` оставлена только практика с витринной моделью CRM-данных.

**Стек и компоненты**
- Apache Airflow
- PostgreSQL DWH
- Grafana

**Архитектура и структура**
- `airflow/` — DAG-и, SQL, init скрипты, `docker-compose.yaml`
- `dataset/` — sample CSV
- `data_generator.py` — генератор данных

Связи: Airflow управляет задачами загрузки и преобразования, PostgreSQL хранит staging, DDS и витрины.

**Практики**
1. **Практика 2 — DAG `crm_report_daily`**
   - STG → DDS → DM
   - Загрузка CSV в staging
   - Построение фактов и витрин

Используемые схемы:
- `stg`, `dds`, `dm`

**DAGS**
**Практика — CRM/DWH витрины маркетинга (crm_report_daily)**
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
3. Доступы к UI:
- Airflow: `http://localhost:8080`
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
  - `AIRFLOW_CONN_POSTGRES_MEDIANATION`

**FAQ / Troubleshooting**
- DAG не видит CSV: проверь `airflow/dags/input/`.
- Логи не отображаются: проверь health контейнеров `airflow-worker`, `airflow-triggerer`, `airflow-dag-processor`.

**Проверка (manual)**
- Поднять `airflow`.
- Скопировать sample CSV и запустить `crm_report_daily`.
