from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, current_timestamp

def main():
    spark = SparkSession.builder \
        .appName("ClientOrdersReport") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://postgres:5432/dwh"

    connection_properties = {
        "user": "airflow", 
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    print("Reading data from raw layer...")
    
    # Читаем из схемы RAW
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

    
    report_df.write.jdbc(
        url=jdbc_url,
        table="mart.client_spend_report",
        mode="append", 
        properties=connection_properties
    )

    print("Job finished successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
