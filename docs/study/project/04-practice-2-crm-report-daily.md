# Практика 2: пайплайн `crm_report_daily`

## Какие файлы формируют эту практику

Главные файлы practice 2:

- `airflow/dags/crm_stats_daily.py`;
- `airflow/scripts/init_dwh.sql`;
- `dataset/*.csv`;
- `data_generator.py`.

Дополнительно важны:

- `airflow/dags/input/` как фактический каталог чтения CSV;
- `airflow/docker-compose.yaml`, где задан connection к `postgres-dwh`.

## Сначала про важное расхождение в именах

Файл DAG-а называется:

- `crm_stats_daily.py`

Но сам DAG внутри имеет ID:

- `crm_report_daily`

Это не ошибка интерпретации, а фактическое устройство кода.
В документации и при работе в Airflow UI это нужно держать в голове отдельно:

- имя Python-файла — одно;
- DAG ID — другое.

## Общая идея practice 2

Эта практика уже ближе к классическому учебному DWH-контуру.
Она строит путь:

`CSV -> stg -> dds -> dm`

В отличие от practice 1:

- здесь нет Spark;
- все данные живут в отдельной базе `postgres-dwh`;
- витрины ориентированы на отчетность по CRM и маркетингу;
- для загрузки и трансформации используются `pandas`, `PostgresHook` и SQL в Python-функциях.

## Где лежат входные данные

### Фактический путь в DAG

В `Config.INPUT_PATH` задано:

- `/opt/airflow/dags/input`

То есть DAG читает файлы не из `dataset/`, а из каталога `airflow/dags/input/`, смонтированного в контейнер Airflow.

### Какие имена ожидаются

Постоянные файлы:

- `clients.csv`
- `campaigns.csv`

Дневные файлы:

- `ad_stats_{date}.csv`
- `site_monitoring_{date}.csv`

Где `{date}` подставляется из Airflow `ds`.

## Важный нюанс с датами sample-файлов

В репозиторном `dataset/` лежат sample-файлы на дату:

- `2026-01-08`

В `airflow/dags/input/` уже лежит другой набор с датой:

- `2026-01-10`

Это значит:

- DAG не подберет дневной CSV автоматически для любой даты;
- логическая дата запуска должна совпасть с именем файлов;
- иначе `read_csv_safe()` завершится `FileNotFoundError`.

Это одна из самых важных практических особенностей practice 2.

## Инициализация DWH через `init_dwh.sql`

Скрипт `airflow/scripts/init_dwh.sql` выполняется при старте сервиса `postgres-dwh`.
Он создает три схемы:

- `stg`
- `dds`
- `dm`

## Слой `stg`

### `stg.clients`

Поля:

- `id TEXT`
- `name TEXT`
- `website TEXT`
- `industry TEXT`
- `manager TEXT`
- `loaded_at TIMESTAMP DEFAULT NOW()`

### `stg.campaigns`

Поля:

- `id TEXT`
- `client_id TEXT`
- `platform TEXT`
- `type TEXT`
- `start_date TEXT`
- `loaded_at TIMESTAMP DEFAULT NOW()`

### `stg.ad_stats`

Поля:

- `date TEXT`
- `campaign_id TEXT`
- `impressions TEXT`
- `clicks TEXT`
- `cost TEXT`
- `conversions TEXT`
- `loaded_at TIMESTAMP DEFAULT NOW()`

### `stg.site_monitoring`

Поля:

- `date TEXT`
- `client_id TEXT`
- `load_time_ms TEXT`
- `uptime_pct TEXT`
- `server_errors TEXT`
- `loaded_at TIMESTAMP DEFAULT NOW()`

### Что означает такой дизайн

`stg` здесь сделан максимально приемочным:

- почти все поля текстовые;
- схема проста;
- сильной валидации на входе нет.

Это удобно для загрузки CSV, но переносит ответственность за типизацию дальше, в SQL трансформации.

## Слой `dds`

### `dds.clients`

Поля:

- `id VARCHAR(50) PRIMARY KEY`
- `name VARCHAR(255)`
- `website VARCHAR(255)`
- `industry VARCHAR(100)`
- `manager VARCHAR(100)`
- `is_active BOOLEAN DEFAULT TRUE`
- `updated_at TIMESTAMP DEFAULT NOW()`

Смысл: текущее состояние клиента в аналитической модели.

### `dds.campaigns`

Поля:

- `id VARCHAR(50) PRIMARY KEY`
- `client_id VARCHAR(50) REFERENCES dds.clients(id)`
- `platform VARCHAR(50)`
- `type VARCHAR(50)`
- `start_date DATE`
- `is_active BOOLEAN DEFAULT TRUE`
- `updated_at TIMESTAMP DEFAULT NOW()`

Смысл: текущее состояние рекламной кампании.

### `dds.fact_advertising`

Поля:

- `id SERIAL PRIMARY KEY`
- `report_date DATE NOT NULL`
- `campaign_id VARCHAR(50) REFERENCES dds.campaigns(id)`
- `impressions INTEGER DEFAULT 0`
- `clicks INTEGER DEFAULT 0`
- `cost DECIMAL(18, 2) DEFAULT 0.0`
- `conversions INTEGER DEFAULT 0`
- `created_at TIMESTAMP DEFAULT NOW()`
- `UNIQUE (report_date, campaign_id)`

Грейн: одна строка на одну кампанию и одну дату отчета.

### `dds.fact_site_health`

Поля:

- `id SERIAL PRIMARY KEY`
- `check_date DATE NOT NULL`
- `client_id VARCHAR(50) REFERENCES dds.clients(id)`
- `load_time_ms INTEGER`
- `uptime_pct DECIMAL(5, 2)`
- `server_errors INTEGER`
- `created_at TIMESTAMP DEFAULT NOW()`
- `UNIQUE (check_date, client_id)`

Грейн: одна строка на одного клиента и одну дату проверки.

## Слой `dm`

### `dm.client_kpi`

Поля:

- `report_month DATE`
- `client_name VARCHAR(255)`
- `industry VARCHAR(100)`
- `total_spend DECIMAL(18, 2)`
- `total_conversions INTEGER`
- `cpl DECIMAL(18, 2)`
- `updated_at TIMESTAMP DEFAULT NOW()`
- `PRIMARY KEY (report_month, client_name)`

Грейн: один клиент и один месяц.

### `dm.platform_stats`

Поля:

- `report_date DATE`
- `platform VARCHAR(50)`
- `avg_ctr DECIMAL(10, 2)`
- `avg_cpc DECIMAL(10, 2)`
- `clicks INTEGER`
- `updated_at TIMESTAMP DEFAULT NOW()`
- `PRIMARY KEY (report_date, platform)`

Грейн: одна платформа и одна дата.

### `dm.site_reliability`

Поля:

- `report_date DATE`
- `website VARCHAR(255)`
- `avg_load_time INTEGER`
- `total_errors INTEGER`
- `status VARCHAR(20)`
- `updated_at TIMESTAMP DEFAULT NOW()`
- `PRIMARY KEY (report_date, website)`

Грейн: один сайт и одна дата.

### `dm.agency_finance`

Поля:

- `report_month DATE`
- `manager VARCHAR(100)`
- `active_clients INTEGER`
- `turnover DECIMAL(18, 2)`
- `commission DECIMAL(18, 2)`
- `updated_at TIMESTAMP DEFAULT NOW()`
- `PRIMARY KEY (report_month, manager)`

Грейн: один менеджер и один месяц.

## Разбор `crm_stats_daily.py`

## Класс `Config`

В начале файла класс `Config` собирает в одном месте:

- DAG ID;
- имя connection;
- путь к входным CSV;
- названия схем;
- имена всех таблиц.

Это хороший локальный прием: не разбрасывать строки вида `dds.fact_advertising` по всему файлу.

Но при этом есть нюанс:

- practice 1 использует отдельный `common/constants.py`;
- practice 2 жестко задает значения в своем собственном классе `Config`.

То есть единый конфигурационный слой для двух практик в репозитории отсутствует.

## Вспомогательные функции

### `get_postgres_engine()`

Берет `PostgresHook(postgres_conn_id=Config.DB_CONN_ID)` и возвращает `get_sqlalchemy_engine()`.
Это нужно для загрузки CSV через `pandas.to_sql`.

### `run_sql()`

Короткая обертка над `hook.run(sql_query, parameters=params)`.
Через нее выполняется вся SQL-логика `dds` и `dm`.

### `read_csv_safe()`

Собирает путь к файлу и заранее проверяет его существование.
Если файла нет, DAG падает сразу и явно.

Это хороший защитный слой, но он также делает запуск очень зависимым от правильной даты в имени файла.

## Шаг 1. Очистка `stg`

Функция `truncate_stg_tables()` выполняет:

- `TRUNCATE TABLE stg.clients, stg.campaigns, stg.ad_stats, stg.site_monitoring;`

Это означает, что staging-слой здесь не историзируется.
Каждый запуск DAG-а воспринимается как новый полный приемочный срез.

## Шаг 2. Загрузка CSV в `stg`

Функция `load_stg_csv()` работает так:

1. получает `ds` из `kwargs`;
2. если в имени файла есть `{date}`, подставляет туда `ds`;
3. читает CSV через `pandas.read_csv`;
4. приводит весь DataFrame к `str` через `astype(str)`;
5. пишет его в PostgreSQL через `df.to_sql(..., if_exists='append')`.

### Почему все приводится к строкам

Потому что `stg` в этой модели — приемочный слой.
Типизация переносится дальше, в SQL для `dds`.

Это упрощает загрузку, но делает качество данных зависимым от следующих шагов.

## Шаг 3. Обновление справочников `dds`

### `load_dds_clients()`

Функция делает две операции:

1. `UPDATE dds.clients SET is_active = FALSE WHERE id NOT IN (SELECT id FROM stg.clients);`
2. `INSERT ... SELECT ... FROM stg.clients ON CONFLICT (id) DO UPDATE ...`

Смысл:

- записи, которых больше нет в текущем входном срезе, помечаются как неактивные;
- записи из текущего среза вставляются или обновляются.

### `load_dds_campaigns()`

Логика та же, но для кампаний.
Здесь дополнительно используется:

- `TO_DATE(start_date, 'YYYY-MM-DD')`

То есть дата кампании типизируется при переносе из `stg` в `dds`.

### Что означает `is_active`

В текущей реализации `is_active` означает не «клиент или кампания активны по бизнес-смыслу», а более узкое:

- «эта сущность присутствует в текущем полном входном срезе справочника».

Это важное смысловое ограничение.

## Шаг 4. Загрузка фактов

### `load_fact_advertising()`

Функция строит дневной факт рекламы в два этапа:

1. `DELETE FROM dds.fact_advertising WHERE report_date = %(date)s;`
2. `INSERT INTO ... SELECT ... FROM stg.ad_stats JOIN dds.campaigns ... WHERE stg.date = %(date)s;`

При вставке выполняются приведения:

- `stg.impressions::INT`
- `stg.clicks::INT`
- `stg.cost::DECIMAL`
- `stg.conversions::INT`
- `TO_DATE(stg.date, 'YYYY-MM-DD')`

Это классический `delete+insert`, а не `upsert`.

### `load_fact_site()`

Логика такая же, но для сайта и мониторинга:

1. удалить дневной срез по `check_date`;
2. заново вставить его из `stg.site_monitoring` после join с `dds.clients`.

### Почему здесь выбран `delete+insert`

Потому что факт естественно пересобирается по одному дневному окну.
Для такого случая `delete+insert` проще и понятнее, чем `upsert`.

## Шаг 5. Построение витрин `dm`

### `calc_dm_platform()`

Формула:

- `AVG(CAST(f.clicks AS DECIMAL) / NULLIF(f.impressions, 0)) * 100` как CTR;
- `AVG(f.cost / NULLIF(f.clicks, 0))` как CPC;
- `SUM(f.clicks)` как объем кликов.

Группировка:

- по дате;
- по платформе.

Запись:

- `INSERT ... ON CONFLICT (report_date, platform) DO UPDATE ...`

То есть дневная витрина поддерживается через `upsert`.

### `calc_dm_reliability()`

Формула:

- `AVG(f.load_time_ms)::INT`;
- `SUM(f.server_errors)`;
- `CASE WHEN SUM(f.server_errors) > 0 THEN 'PROBLEM' ELSE 'OK' END`.

Группировка:

- по дате;
- по website.

Запись:

- тоже через `ON CONFLICT`.

### `calc_dm_kpi()`

Эта витрина уже месячная.
Функция делает:

1. `DELETE` по `report_month = DATE_TRUNC('month', %(date)s::date)`;
2. `INSERT` агрегированного среза за месяц.

Метрики:

- `SUM(f.cost)` как `total_spend`;
- `SUM(f.conversions)` как `total_conversions`;
- `SUM(f.cost) / NULLIF(SUM(f.conversions), 0)` как `cpl`.

### `calc_dm_finance()`

Тоже месячная витрина.
Метрики:

- `COUNT(DISTINCT c.id)` как `active_clients`;
- `SUM(f.cost)` как `turnover`;
- `SUM(f.cost) * 0.10` как `commission`.

Здесь `active_clients` — это число клиентов, у которых были рекламные факты в месяце, а не флаг `is_active` из `dds.clients`.

## DAG-зависимости и что они означают

Фактический граф зависимостей:

1. `t_truncate`
2. параллельно `t_stg_clients`, `t_stg_campaigns`, `t_stg_stats`, `t_stg_monitor`
3. после всех четырех — `t_dds_clients`
4. после `t_dds_clients` — `t_dds_campaigns`
5. после `t_dds_campaigns` — `t_dds_ad_facts`
6. после `t_dds_clients` — `t_dds_site_facts`
7. после `t_dds_ad_facts` — `t_dm_kpi`, `t_dm_platform`, `t_dm_finance`
8. после `t_dds_site_facts` — `t_dm_reliability`

### Что здесь сделано чуть шире, чем необходимо

`t_dds_clients` ждет завершения всех четырех STG-загрузок, хотя по самой логике ему нужен только `stg.clients`.
Это не критично, но показывает, что DAG сделан с акцентом на простоту зависимости, а не на минимальный критический путь.

## Как sample-данные соотносятся с моделью

По `dataset/` и `data_generator.py` видно:

- `clients.csv` содержит 200 клиентов;
- `campaigns.csv` содержит 500 кампаний;
- дневной `ad_stats_*.csv` содержит 400 записей;
- дневной `site_monitoring_*.csv` содержит 200 записей.

Это совпадает с логикой генератора:

- справочники создаются один раз;
- факты генерируются на дату;
- для рекламы активной считается только часть кампаний;
- для мониторинга строится одна строка на клиента в день.

## Как данные становятся пригодными для Grafana

Именно слой `dm` делает данные практически готовыми к визуализации.
Причина простая:

- в `dm` уже есть дневные и месячные срезы;
- ключевые метрики посчитаны;
- зерно витрин стабильное и понятное;
- Grafana не пришлось бы рассчитывать CTR, CPC, CPL и статус надежности самостоятельно.

Поэтому если Grafana будет реально подключена к проекту, ее естественной точкой входа являются таблицы `dm`, а не `stg` и не `dds`.

## Главные ограничения и учебные упрощения

### 1. Нет полноценной историзации измерений

`dds.clients` и `dds.campaigns` хранят только текущее актуальное состояние с `is_active`, а не полноценный SCD.

### 2. Staging полностью текстовый

Это упрощает приемку CSV, но делает качество данных зависимым от SQL-кастов на следующих шагах.

### 3. Дневные файлы привязаны к `ds`

Если логическая дата DAG-а не совпадет с доступными файлами, пайплайн упадет еще на чтении CSV.

### 4. Инициализация DWH вынесена не в DAG, а в container init

Скрипт `init_dwh.sql` выполняется при старте базы.
Сам DAG не создает схемы и таблицы самостоятельно.

### 5. Функции `build_mart_daily` и `build_mart_monthly` названы шире, чем их реальная роль

Они не строят витрины сами по себе, а просто исполняют переданный SQL-шаблон с параметром даты.

## Итог

`crm_report_daily` — это основной учебный DWH-пайплайн репозитория.
Он уже показывает реальные паттерны аналитической загрузки:

- `stg` как приемочный слой;
- `dds` как детализированная модель;
- `dm` как слой витрин;
- `deactivate + upsert` для справочников;
- `delete+insert` для фактов и месячных витрин;
- `ON CONFLICT` для части дневных витрин.

При этом нужно честно помнить, что это все еще учебная реализация:
с хорошей архитектурной логикой, но без полной production-строгости по историзации, конфигурации и качеству входа.

