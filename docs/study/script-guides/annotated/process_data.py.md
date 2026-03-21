# Аннотированная копия `process_data.py`

## 1. Импорты

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, current_timestamp
```

Почему `sum` переименован в `_sum`:

- в Python уже есть встроенная функция `sum`;
- переименование убирает конфликт имен и делает код чуть безопаснее.

Импортируются только те функции Spark SQL, которые реально нужны:

- агрегат суммы;
- ссылка на колонку;
- текущая временная метка.

## 2. Создание `SparkSession`

```python
def main():
    spark = SparkSession.builder \
        .appName("ClientOrdersReport") \
        .getOrCreate()
```

Что делает этот блок:

- создает точку входа в Spark-приложение;
- задает имя job для UI и логов;
- либо создает новую сессию, либо берет существующую.

Весь скрипт обернут в функцию `main()`, что делает его аккуратным обычным Python-entrypoint.

## 3. JDBC-настройки

```python
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"

    connection_properties = {
        "user": "airflow", 
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
```

Это один из ключевых моментов всего файла.

Что важно понимать:

- Spark подключается к PostgreSQL не через Airflow connection, а напрямую;
- hostname `postgres` работает только потому, что Spark и Airflow находятся в общей Docker-сети;
- credentials зашиты в код, что приемлемо для учебного проекта, но не для зрелой конфигурации.

## 4. Чтение `raw`-таблиц

```python
    clients_df = spark.read.jdbc(
        url=jdbc_url,
        table="raw.clients",
        properties=connection_properties
    )

    orders_df = spark.read.jdbc(
        url=jdbc_url,
        table="raw.orders",
        properties=connection_properties
    )
```

Здесь Spark читает PostgreSQL как внешний реляционный источник.

Что важно по смыслу:

- `clients_df` — это измерение клиентов;
- `orders_df` — это факт заказов;
- оба DataFrame создаются до начала трансформации.

## 5. Join и агрегация

```python
    joined_df = orders_df.join(
        clients_df,
        orders_df.client_id == clients_df.id,
        "inner"
    )

    report_df = joined_df.groupBy("name") \
        .agg(_sum("amount").alias("total_spent")) \
        .withColumn("generated_at", current_timestamp()) \
        .withColumnRenamed("name", "client_name") \
        .orderBy(col("total_spent").desc())
```

Как читать этот фрагмент:

- `inner join` отбрасывает заказы без существующего клиента;
- `groupBy("name")` делает одну строку на клиента;
- `_sum("amount")` считает общий объем трат;
- `alias("total_spent")` задает имя итоговой метрики;
- `withColumn("generated_at", current_timestamp())` добавляет техническую метку времени расчета;
- `withColumnRenamed("name", "client_name")` приводит итоговую колонку к имени витрины;
- `orderBy(...)` делает результат отсортированным по убыванию трат.

Стоит отдельно заметить, что группировка идет по `name`, а не по `id`.
В текущей генерации имен этого достаточно, но для более строгой модели обычно безопаснее опираться на устойчивый идентификатор.

## 6. Запись результата в `mart`

```python
    report_df.write.jdbc(
        url=jdbc_url,
        table="mart.client_spend_report",
        mode="append", 
        properties=connection_properties
    )
```

Это самый важный operational-момент файла.

`mode="append"` означает:

- Spark не очищает витрину;
- Spark не обновляет существующий набор;
- каждый новый запуск просто дописывает строки.

Именно из-за этого practice 1 не является идемпотентной на уровне `mart`.

## 7. Завершение приложения

```python
    print("Job finished successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
```

Здесь:

- выводится простое сообщение в логи;
- корректно закрывается `SparkSession`;
- файл остается стандартным исполняемым Python-скриптом.

## Главная мысль по скрипту

`process_data.py` — очень маленький, но очень показательный Spark job.
Он демонстрирует весь базовый маршрут practice 1:

- JDBC read из PostgreSQL;
- DataFrame `join`;
- `groupBy` и `agg`;
- запись обратно в PostgreSQL.

Именно поэтому этот файл стоит читать не как «тривиальный код на десять строк», а как минимальный учебный пример отдельного вычислительного слоя в архитектуре проекта.

