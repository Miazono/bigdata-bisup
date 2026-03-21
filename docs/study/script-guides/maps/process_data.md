# Карта `process_data.py`

## Назначение

Spark job practice 1.
Он читает `raw`-таблицы из PostgreSQL, агрегирует траты клиентов и записывает результат в `mart`.

## Входы

- `raw.clients`
- `raw.orders`
- JDBC URL `jdbc:postgresql://postgres:5432/airflow`
- JDBC driver `org.postgresql.Driver`

## Выход

- `mart.client_spend_report`

## Ключевые шаги

1. Создать `SparkSession`.
2. Настроить JDBC connection properties.
3. Прочитать `raw.clients`.
4. Прочитать `raw.orders`.
5. Выполнить `inner join`.
6. Посчитать `sum(amount)` по клиенту.
7. Добавить `generated_at`.
8. Записать результат через `write.jdbc(..., mode="append")`.

## Что важно помнить

- Скрипт не параметризует host, user и password.
- Режим записи `append` делает витрину неидемпотентной без дополнительной очистки.
- Spark здесь считает только один отчет, а не универсальный набор трансформаций.

