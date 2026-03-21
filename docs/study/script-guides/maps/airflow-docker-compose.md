# Карта `airflow/docker-compose.yaml`

## Назначение

Файл поднимает основной локальный контур проекта:

- Airflow cluster;
- Redis;
- `postgres`;
- `postgres-dwh`;
- Grafana.

## Главные секции

- `x-airflow-common` с общей конфигурацией сервисов Airflow;
- service `postgres`;
- service `redis`;
- сервисы `airflow-*`;
- service `postgres-dwh`;
- service `grafana`;
- volumes;
- network `shared-data-network`.

## Ключевые connections

- `AIRFLOW_CONN_DWH_POSTGRES`
- `AIRFLOW_CONN_POSTGRES_MEDIANATION`
- `AIRFLOW_CONN_SPARK_DEFAULT`

## Ключевые mounts

- `./dags`
- `./logs`
- `./config`
- `./plugins`
- `../spark/apps -> /opt/spark/work-dir`

## Что важно помнить

- `postgres` хранит и metadata Airflow, и данные practice 1.
- `postgres-dwh` хранит practice 2.
- Grafana поднята, но не provisioned.
- сеть `shared-data-network` нужна для связи с отдельным Spark compose.

