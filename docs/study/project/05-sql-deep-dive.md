# Подробный разбор SQL в проекте

## Какие SQL-слои вообще есть в репозитории

В проекте SQL живет не в одном месте, а в трех разных формах:

1. отдельные `.sql`-файлы для bootstrap practice 1;
2. `init_dwh.sql` для инициализации practice 2;
3. SQL-строки внутри `crm_stats_daily.py`.

Это важно понимать заранее.
Если смотреть только на каталог `airflow/dags/sql/`, кажется, что SQL-логики мало.
На деле основная DML-логика practice 2 зашита прямо в Python-файл DAG-а.

## Practice 1: SQL bootstrap для `raw`

## `create_raw_tables.sql`

Файл делает три вещи:

1. `CREATE SCHEMA IF NOT EXISTS raw;`
2. `CREATE TABLE IF NOT EXISTS raw.clients (...)`
3. `CREATE TABLE IF NOT EXISTS raw.orders (...)`

После этого выполняются:

- `TRUNCATE TABLE raw.orders RESTART IDENTITY CASCADE;`
- `TRUNCATE TABLE raw.clients RESTART IDENTITY CASCADE;`

### Что это означает по смыслу

- `raw.clients` и `raw.orders` могут существовать между запусками контейнера;
- каждый запуск DAG-а все равно очищает их перед новой генерацией данных;
- `RESTART IDENTITY` сбрасывает `SERIAL`, чтобы `id` начинались заново.

Порядок очистки тоже важен:

- сначала `raw.orders`;
- затем `raw.clients`.

Такой порядок естественен, потому что `raw.orders.client_id` связан внешним ключом с `raw.clients.id`.

## `init_db.sql`

Этот файл почти повторяет `create_raw_tables.sql`.
Он тоже:

- создает `raw`;
- создает `raw.clients`;
- создает `raw.orders`;
- очищает таблицы.

Разница в том, что здесь очистка без `RESTART IDENTITY`.

### Что это говорит об архитектуре

В practice 1 есть дублирование ответственности:

- container init создает `raw`-таблицы при старте `postgres`;
- DAG снова делает похожий bootstrap при выполнении.

С учебной точки зрения это не страшно, но при чтении проекта важно видеть, что логика создания `raw` описана в двух местах.

## Practice 1: SQL bootstrap для `mart`

## `create_mart_tables.sql`

Файл:

- создает схему `mart`;
- создает таблицу `mart.client_spend_report`.

DDL очень минималистичен:

- нет `PRIMARY KEY`;
- нет `UNIQUE`;
- нет индексов;
- нет явной очистки таблицы.

### Почему это важно

Вся корректность загрузки `mart` здесь держится не на SQL-ограничениях, а на поведении Spark job.
Но Spark пишет в `append`, поэтому SQL-слой сам по себе не защищает витрину от дублей.

## Practice 2: `init_dwh.sql`

Это основной SQL-файл practice 2.
Он описывает целый мини-DWH.

## Схемы

Создаются:

- `stg`;
- `dds`;
- `dm`.

Это уже не просто вспомогательные таблицы, а явная слоистая архитектура внутри одной базы.

## Staging-таблицы

Все staging-таблицы построены по одному принципу:

- бизнес-поля хранятся как `TEXT`;
- есть `loaded_at TIMESTAMP DEFAULT NOW()`.

Это означает, что `stg` понимается как приемочный, а не аналитический слой.

## DDS-таблицы

В `dds` появляются уже полноценные типы и ограничения:

- `PRIMARY KEY`;
- `FOREIGN KEY`;
- `BOOLEAN`;
- `DATE`;
- `DECIMAL`;
- `UNIQUE` по естественному grain фактов.

Именно здесь SQL переходит от хранения «как пришло» к хранению «как должно использоваться».

## DM-таблицы

В `dm` лежат витрины с составными первичными ключами:

- месяц + клиент;
- день + платформа;
- день + сайт;
- месяц + менеджер.

Это важный сигнал:
grain витрин фиксируется не только логически, но и на уровне ограничения таблицы.

## SQL внутри `crm_stats_daily.py`

## Паттерн 1. `deactivate + upsert` для справочников

### `load_dds_clients()`

Сначала:

```sql
UPDATE dds.clients
SET is_active = FALSE
WHERE id NOT IN (SELECT id FROM stg.clients);
```

Потом:

```sql
INSERT INTO dds.clients (...)
SELECT ...
FROM stg.clients
ON CONFLICT (id) DO UPDATE SET ...
```

### Как это читать

Шаг состоит из двух логик:

- отметить отсутствующие в текущем входе записи как неактивные;
- актуальный входной срез вставить или обновить.

Это не SCD и не хранение истории изменений.
Это модель «текущее состояние + флаг присутствия в последнем срезе».

### `load_dds_campaigns()`

Паттерн тот же.
Отдельно важно:

- `TO_DATE(start_date, 'YYYY-MM-DD')`

То есть дата типизируется именно в момент переноса из `stg` в `dds`.

## Паттерн 2. `delete+insert` для фактов

### `load_fact_advertising()`

Сначала:

```sql
DELETE FROM dds.fact_advertising WHERE report_date = %(date)s;
```

Потом вставка:

```sql
INSERT INTO dds.fact_advertising (...)
SELECT
    TO_DATE(stg.date, 'YYYY-MM-DD'),
    stg.campaign_id,
    stg.impressions::INT,
    stg.clicks::INT,
    stg.cost::DECIMAL,
    stg.conversions::INT
FROM stg.ad_stats stg
JOIN dds.campaigns d ON stg.campaign_id = d.id
WHERE stg.date = %(date)s;
```

### Что здесь важно

- используется параметризация по дате;
- все числовые поля типизируются через `::`;
- факт строится только из тех кампаний, которые существуют в `dds.campaigns`;
- join одновременно выполняет и интеграционную, и валидационную роль.

### `load_fact_site()`

Логика симметрична:

- удалить дневной срез;
- заново вставить его из `stg.site_monitoring`;
- привести типы;
- проверить существование клиента через join с `dds.clients`.

## Паттерн 3. Upsert для дневных витрин

### `calc_dm_platform()`

Ключевой SQL-фрагмент:

```sql
INSERT INTO dm.platform_stats (report_date, platform, avg_ctr, avg_cpc, clicks)
SELECT
    f.report_date,
    c.platform,
    AVG(CAST(f.clicks AS DECIMAL) / NULLIF(f.impressions, 0)) * 100,
    AVG(f.cost / NULLIF(f.clicks, 0)),
    SUM(f.clicks)
FROM dds.fact_advertising f
JOIN dds.campaigns c ON f.campaign_id = c.id
WHERE f.report_date = %(date)s
GROUP BY 1, 2
ON CONFLICT (report_date, platform) DO UPDATE SET ...
```

Что важно:

- `GROUP BY 1, 2` — позиционная запись группировки по первому и второму выражению из `SELECT`;
- `NULLIF` защищает от деления на ноль;
- `CAST(... AS DECIMAL)` позволяет не потерять точность в CTR;
- витрина обновляется по естественному grain `date + platform`.

### `calc_dm_reliability()`

Аналогично:

- `AVG(f.load_time_ms)::INT`;
- `SUM(f.server_errors)`;
- `CASE WHEN SUM(f.server_errors) > 0 THEN 'PROBLEM' ELSE 'OK' END`;
- `GROUP BY 1, 2`;
- `ON CONFLICT (report_date, website) DO UPDATE`.

Это хороший пример того, как статус витрины вычисляется из факта, а не приходит из источника готовым.

## Паттерн 4. `delete+insert` для месячных витрин

### `calc_dm_kpi()`

Сначала удаление месяца:

```sql
DELETE FROM dm.client_kpi
WHERE report_month = DATE_TRUNC('month', %(date)s::date);
```

Потом вставка месячного агрегата:

- `SUM(f.cost)` как `total_spend`;
- `SUM(f.conversions)` как `total_conversions`;
- `SUM(f.cost) / NULLIF(SUM(f.conversions), 0)` как `cpl`;
- `GROUP BY 1, 2, 3`.

### `calc_dm_finance()`

Тот же паттерн:

- удалить месячный срез;
- заново вставить его.

Здесь отдельно важно:

- `COUNT(DISTINCT c.id)` как число клиентов в месячном окне;
- `SUM(f.cost) * 0.10` как фиксированная комиссия.

## Какие SQL-конструкции реально используются

В проекте действительно встречаются:

- `CREATE SCHEMA IF NOT EXISTS`;
- `CREATE TABLE IF NOT EXISTS`;
- `PRIMARY KEY`;
- `FOREIGN KEY`;
- `UNIQUE`;
- `SERIAL`;
- `VARCHAR`, `TEXT`, `DATE`, `TIMESTAMP`, `DECIMAL`, `BOOLEAN`;
- `DEFAULT NOW()` и `DEFAULT CURRENT_TIMESTAMP`;
- `TRUNCATE`, `RESTART IDENTITY`, `CASCADE`;
- `INSERT INTO ... SELECT`;
- `UPDATE`;
- `DELETE`;
- `JOIN`;
- `GROUP BY` в обычной и позиционной форме;
- `CASE`;
- `AVG`, `SUM`, `COUNT(DISTINCT ...)`;
- `NULLIF`;
- `TO_DATE`;
- `DATE_TRUNC`;
- `CAST` и `::`.

И при этом не используются:

- `MERGE`;
- оконные функции;
- CTE;
- stored procedures;
- materialized views;
- партиционирование.

## Что важно понимать про SQL-стиль проекта

### 1. SQL довольно прямолинейный

Логика почти всегда читается сверху вниз без сложных подзапросов и без CTE.
Это полезно для учебной читаемости.

### 2. Основная DML-логика practice 2 живет прямо в DAG-файле

Это удобно для компактности репозитория, но усложняет повторное использование SQL отдельно от Python.

### 3. Grain часто поддерживается ограничениями таблиц

У фактов и витрин есть составные `UNIQUE` или `PRIMARY KEY`, что хорошо сочетается с `upsert`.

### 4. Идемпотентность достигается не одним способом

Проект одновременно использует:

- `TRUNCATE`;
- `delete+insert`;
- `upsert`.

Это хороший учебный пример того, что разные слои требуют разных стратегий загрузки.

## Главные ограничения SQL-реализации

- В practice 1 итоговая витрина не защищена SQL-ограничениями от дублей.
- В practice 2 нет индексов сверх тех, что приходят с `PRIMARY KEY` и `UNIQUE`.
- В репозитории нет отдельного каталога SQL-модулей: часть логики лежит в файлах, часть внутри Python.
- `init_db.sql` и `create_raw_tables.sql` дублируют друг друга по смыслу.

## Итог

SQL в этом проекте несложный по синтаксису, но очень полезный для изучения типовых DWH-паттернов:

- bootstrap слоев;
- приведение типов;
- `upsert`;
- `delete+insert`;
- агрегаты для витрин;
- работа с grain и ключами.

Именно поэтому practice 2 особенно хороша как учебный материал по SQL внутри аналитического пайплайна.

