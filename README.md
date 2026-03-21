# CRM DWH Demo

README оставлен только с практической информацией:
- как запустить стенд;
- как подготовить raw-данные для DAG `crm_report_daily`.

## Запуск
Запуск стенда:

```bash
cd airflow
./init.sh
docker compose up -d --build
```

После старта будут доступны:
- Airflow: `http://localhost:8080`
- Grafana: `http://localhost:3000`

## Raw-данные

DAG `crm_report_daily` читает CSV из каталога `airflow/dags/input/`.

Обязательные файлы:
- `clients.csv`
- `campaigns.csv`
- `ad_stats_YYYY-MM-DD.csv`
- `site_monitoring_YYYY-MM-DD.csv`

Важно:
- дата в имени файлов `ad_stats_YYYY-MM-DD.csv` и `site_monitoring_YYYY-MM-DD.csv` должна совпадать с датой запуска DAG;
- если запускать DAG за дату `2026-01-10`, в папке должны лежать `ad_stats_2026-01-10.csv` и `site_monitoring_2026-01-10.csv`.


Пример генерации набора за `2026-01-10` сразу в папку, откуда их читает Airflow:

```bash
mkdir -p airflow/dags/input
python3 data_generator.py --date 2026-01-10 --output airflow/dags/input
```

Опциональные параметры генератора:
- `--clients` — количество клиентов, по умолчанию `300`;
- `--campaigns` — количество кампаний, по умолчанию `1230`;
- `--output` — каталог для сохранения файлов, по умолчанию `dataset`.

Пример с изменением объема данных:

```bash
python3 data_generator.py \
  --date 2026-01-10 \
  --clients 500 \
  --campaigns 2000 \
  --output airflow/dags/input
```

## Запуск в Airflow

1. Открыть UI Airflow: `http://localhost:8080`
2. Найти DAG `crm_report_daily`
3. Запустить его с датой, для которой подготовлены daily-файлы.
