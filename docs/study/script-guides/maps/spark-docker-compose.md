# Карта `spark/docker-compose.yaml`

## Назначение

Файл поднимает отдельный Spark-контур:

- `master`;
- `worker`.

## Что настраивает `master`

- образ `custom-spark:3.5.1`;
- порт `7077` для Spark master;
- web UI на `5080`;
- volume `./apps -> /opt/spark/work-dir`.

## Что настраивает `worker`

- подключение к `spark://master:7077`;
- `--cores 1`;
- `--memory 2G`;
- web UI на `8081`.

## Сеть

Используется внешняя сеть:

- `shared-data-network`

Это позволяет Spark видеть сервисы из Airflow compose и наоборот.

## Что важно помнить

- Spark intentionally вынесен из Airflow compose;
- `apps/` и `spark-data/` пробрасываются в контейнеры;
- для practice 1 это обязательная часть вычислительного контура.

