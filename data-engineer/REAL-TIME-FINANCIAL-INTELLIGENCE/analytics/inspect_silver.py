#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InspectSilverTransactions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

path = "hdfs://localhost:9000/financial-intelligence/silver/transactions/"

df = spark.read.parquet(path)

print("========== SILVER SCHEMA ==========")
df.printSchema()

print("========== EVENT TIME SAMPLE ==========")
df.select(
    "transaction_id",
    "customer_id",
    "event_time",
    "amount",
    "transaction_type"
).show(10, truncate=False)

spark.stop()
