# Схемы движения данных и точки интеграции

## Зачем разбирать потоки отдельно от файлов

Даже если отдельно понятны DAG-и, SQL и compose-конфиги, без общей схемы легко потерять сам путь данных.
Этот документ нужен именно для того, чтобы собрать интегральную картину:

- откуда данные приходят;
- куда они сначала попадают;
- где именно выполняются преобразования;
- в какой момент подключаются Spark и Grafana.

## Общая картина репозитория

В проекте есть три логических потока:

1. инфраструктурный поток между сервисами;
2. бизнесовый поток practice 1;
3. бизнесовый поток practice 2.

## Инфраструктурный поток

На уровне сервисов общая схема выглядит так:

1. Airflow scheduler планирует задачи.
2. Airflow worker исполняет задачи через Celery.
3. Redis выступает брокером очереди.
4. `postgres` хранит metadata Airflow и данные practice 1.
5. `postgres-dwh` хранит данные practice 2.
6. Spark master и worker исполняют вычисления practice 1.
7. Grafana потенциально читает `dm` из `postgres-dwh`.

Объединяет все это сеть `shared-data-network`.

## Поток practice 1: `raw -> mart`

### Шаг 1. Подготовка `raw`

Airflow task `create_raw_tables` создает схему `raw`, таблицы `raw.clients` и `raw.orders`, затем очищает их.

### Шаг 2. Подготовка `mart`

Airflow task `create_mart_tables` создает схему `mart` и таблицу `mart.client_spend_report`.

### Шаг 3. Генерация данных

Airflow task `generate_data` через `PostgresHook` и `execute_values` записывает:

- 100 клиентов;
- 500 заказов.

Обе таблицы находятся в сервисе `postgres`.

### Шаг 4. Передача в Spark

Airflow task `process_client_spend` использует `SparkSubmitOperator`.
Он обращается к Spark master по connection `spark_default` и запускает:

- `/opt/spark/work-dir/process_data.py`

### Шаг 5. Вычисления в Spark

Spark job:

- читает `raw.clients` и `raw.orders` по JDBC;
- соединяет их;
- агрегирует траты по клиенту;
- добавляет `generated_at`;
- пишет результат обратно в `mart.client_spend_report`.

### Итоговый поток practice 1

```text
Airflow SQL -> raw
Airflow Python -> raw
Airflow SparkSubmit -> Spark
Spark JDBC read -> raw
Spark JDBC write -> mart
```

## Точки интеграции practice 1

- Airflow и PostgreSQL связаны через connection `dwh_postgres`.
- Airflow и Spark связаны через connection `spark_default`.
- Spark и PostgreSQL связаны через hardcoded JDBC URL.
- Airflow и Spark делят каталог `spark/apps` через volume.

## Главный архитектурный эффект practice 1

Practice 1 хорошо показывает разделение:

- orchestration в Airflow;
- compute в Spark;
- storage в PostgreSQL.

Но одновременно показывает и ограничение:

- `mart` пишется append-only и не очищается при повторном запуске.

## Поток practice 2: `stg -> dds -> dm`

### Шаг 1. Очистка staging

Task `truncate_stg` очищает:

- `stg.clients`
- `stg.campaigns`
- `stg.ad_stats`
- `stg.site_monitoring`

### Шаг 2. Приемка CSV

Четыре task-а параллельно читают:

- `clients.csv`
- `campaigns.csv`
- `ad_stats_{ds}.csv`
- `site_monitoring_{ds}.csv`

Файлы читаются из `airflow/dags/input/`, а не из `dataset/`.

### Шаг 3. Построение измерений

`load_dds_clients`:

- деактивирует отсутствующие в текущем срезе `id`;
- вставляет или обновляет клиентов.

`load_dds_campaigns`:

- делает то же самое для кампаний;
- типизирует `start_date`.

### Шаг 4. Построение фактов

`load_dds_ad_facts`:

- удаляет дневной срез из `dds.fact_advertising`;
- заново вставляет его из `stg.ad_stats`.

`load_dds_site_facts`:

- удаляет дневной срез из `dds.fact_site_health`;
- заново вставляет его из `stg.site_monitoring`.

### Шаг 5. Построение витрин

После рекламного факта строятся:

- `dm.client_kpi`
- `dm.platform_stats`
- `dm.agency_finance`

После факта мониторинга строится:

- `dm.site_reliability`

### Итоговый поток practice 2

```text
CSV -> stg
stg -> dds dimensions
stg + dds -> dds facts
dds facts + dimensions -> dm
```

## Точки интеграции practice 2

- Airflow читает CSV с локального volume `dags/input`.
- `pandas` + SQLAlchemy engine пишут `stg` в `postgres-dwh`.
- SQL через `PostgresHook.run` строит `dds` и `dm`.
- Grafana потенциально может читать уже готовый `dm`.

## Где в practice 2 проходит граница между слоями

### Приемка

`CSV -> stg`

Здесь задача — принять вход почти как есть.

### Моделирование

`stg -> dds`

Здесь появляются:

- типы;
- ключи;
- связи;
- grain фактов.

### Потребление

`dds -> dm`

Здесь появляются:

- CTR;
- CPC;
- CPL;
- месячные KPI;
- надежность сайтов;
- финансовые показатели агентства.

## Общие точки интеграции двух практик

Несмотря на различие сценариев, обе практики опираются на общие принципы:

- orchestration в Airflow;
- хранение в PostgreSQL;
- использование volume для доступа к входным артефактам;
- зависимость от корректных connection и hostname внутри Docker network.

## Где подключается Grafana

По фактическому коду Grafana еще не включена в автоматический бизнесовый поток.
Но логическая точка интеграции очевидна:

- она должна читать `dm`-таблицы из `postgres-dwh`.

То есть Grafana стоит не на середине пайплайна, а после него, на выходе.

## Основные различия потоков

### Practice 1

- один короткий маршрут;
- Spark как внешний вычислительный шаг;
- `raw -> mart`;
- итоговая таблица одна.

### Practice 2

- более длинный маршрут;
- без Spark;
- `stg -> dds -> dm`;
- несколько витрин с разным grain.

## Итог

Если смотреть на репозиторий через потоки данных, становится особенно хорошо видно, что это не «один большой DWH-проект», а учебный стенд из двух разных сценариев:

- один показывает интеграцию Airflow и Spark;
- второй показывает более зрелую SQL-ориентированную DWH-модель.

