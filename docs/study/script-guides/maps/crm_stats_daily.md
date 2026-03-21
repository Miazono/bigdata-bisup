# Карта `crm_stats_daily.py`

## Назначение

Файл описывает DAG practice 2.
Он:

- очищает `stg`;
- загружает CSV;
- строит `dds`;
- строит витрины `dm`.

## Где используется

- имя файла: `crm_stats_daily.py`
- DAG ID внутри: `crm_report_daily`

## Входы

- connection `postgres_medianation`
- `clients.csv`
- `campaigns.csv`
- `ad_stats_{ds}.csv`
- `site_monitoring_{ds}.csv`
- каталог `/opt/airflow/dags/input`

## Выходы

- `stg.clients`
- `stg.campaigns`
- `stg.ad_stats`
- `stg.site_monitoring`
- `dds.clients`
- `dds.campaigns`
- `dds.fact_advertising`
- `dds.fact_site_health`
- `dm.client_kpi`
- `dm.platform_stats`
- `dm.site_reliability`
- `dm.agency_finance`

## Ключевые шаги

1. Очистить `stg`.
2. Прочитать CSV через `pandas`.
3. Записать `stg` через `to_sql`.
4. Обновить справочники через `deactivate + upsert`.
5. Пересобрать факты через `delete+insert`.
6. Построить витрины `dm`.

## Что важно помнить

- DAG зависит от `ds`, потому что дневные файлы выбираются по дате в имени.
- `stg` полностью текстовый.
- `dds` хранит текущее состояние, а не полноценную историю измерений.
- дневные и месячные витрины обновляются разными паттернами.

