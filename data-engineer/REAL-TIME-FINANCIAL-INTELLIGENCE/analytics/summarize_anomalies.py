#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum, round as spark_round

spark = (
    SparkSession.builder
    .appName("SummarizeAnomalies")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

path = "hdfs://localhost:9000/financial-intelligence/analytics/anomalies/"

df = spark.read.parquet(path)

print("========== FLAG REASONS ==========")

(
    df.groupBy("anomaly_reason")
    .agg(
        count("*").alias("records"),
        spark_round(spark_sum("amount"), 2).alias("total_amount")
    )
    .orderBy("anomaly_reason")
    .show(truncate=False)
)

print("Total flagged records:", df.count())

spark.stop()
