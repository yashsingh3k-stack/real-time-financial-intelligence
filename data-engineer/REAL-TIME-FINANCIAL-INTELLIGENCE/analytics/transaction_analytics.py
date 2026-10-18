#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    round as spark_round
)

spark = (
    SparkSession.builder
    .appName("FinancialTransactionAnalytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/financial-intelligence"
silver_path = base + "/silver/transactions/"

df = spark.read.parquet(silver_path)

print("\n========== DATASET SUMMARY ==========")
print("Total transactions:", df.count())

print("\n========== TRANSACTION TYPES ==========")
(
    df.groupBy("transaction_type")
    .agg(
        count("*").alias("transaction_count"),
        spark_round(spark_sum("amount"), 2).alias("total_amount"),
        spark_round(avg("amount"), 2).alias("average_amount")
    )
    .orderBy("transaction_type")
    .show(truncate=False)
)

print("\n========== HIGH-VALUE TRANSACTIONS (>= 40000 INR) ==========")
(
    df.filter(
        (col("currency") == "INR") &
        (col("amount") >= 40000)
    )
    .select(
        "transaction_id",
        "customer_id",
        "amount",
        "city",
        "merchant_category",
        "transaction_type"
    )
    .orderBy(col("amount").desc())
    .show(20, truncate=False)
)

print("\n========== TRANSACTIONS BY CITY ==========")
(
    df.groupBy("city")
    .agg(
        count("*").alias("transaction_count"),
        spark_round(spark_sum("amount"), 2).alias("total_amount")
    )
    .orderBy(col("transaction_count").desc())
    .show(truncate=False)
)

print("\n========== TRANSACTIONS BY MERCHANT CATEGORY ==========")
(
    df.groupBy("merchant_category")
    .agg(
        count("*").alias("transaction_count"),
        spark_round(spark_sum("amount"), 2).alias("total_amount")
    )
    .orderBy(col("total_amount").desc())
    .show(truncate=False)
)

spark.stop()
