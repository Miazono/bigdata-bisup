# Script guides

## Назначение

Раздел `script-guides/` — это вспомогательный быстрый формат.
Он нужен, когда надо быстро восстановить в памяти назначение файла, его входы, выходы и последовательность шагов, а не читать длинный проектный разбор целиком.

## Форматы внутри раздела

- `maps/` — короткие карты файлов: назначение, зависимости, шаги, входы и выходы;
- `annotated/` — аннотированные копии ключевых Python-файлов с пояснениями по блокам кода.

## Состав раздела

### Карты файлов

- [Карта `elt_pipeline.py`](maps/elt_pipeline.md)
- [Карта `crm_stats_daily.py`](maps/crm_stats_daily.md)
- [Карта `process_data.py`](maps/process_data.md)
- [Карта `init_dwh.sql`](maps/init_dwh.md)
- [Карта `docker-compose.yaml` для Airflow](maps/airflow-docker-compose.md)
- [Карта `docker-compose.yaml` для Spark](maps/spark-docker-compose.md)

### Аннотированные копии

- [Аннотированная копия `elt_pipeline.py`](annotated/elt_pipeline.py.md)
- [Аннотированная копия `crm_stats_daily.py`](annotated/crm_stats_daily.py.md)
- [Аннотированная копия `process_data.py`](annotated/process_data.py.md)

## Как соотносить с остальными разделами

- если нужен полный смысл и причины решений, сначала читать `project/`;
- если нужно быстро вспомнить устройство файла перед чтением кода, достаточно `script-guides/`.
