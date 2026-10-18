#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = (
    "hdfs://localhost:9000/"
    "financial-intelligence/million_run/"
    "silver/transactions/batch_id=0"
)

spark = (
    SparkSession.builder
    .appName("CustomerActivityAnalysis")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(SILVER_PATH)

# Calculate transaction counts per customer
activity = (
    df.groupBy("customer_id")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_amount")
    )
)

print("\n=== CUSTOMER ACTIVITY DISTRIBUTION ===")

activity.select(
    F.count("*").alias("unique_customers"),
    F.round(F.avg("transaction_count"), 2).alias("average_transactions"),
    F.min("transaction_count").alias("minimum_transactions"),
    F.max("transaction_count").alias("maximum_transactions"),
    F.expr(
        "percentile_approx(transaction_count, 0.50)"
    ).alias("median_transactions"),
    F.expr(
        "percentile_approx(transaction_count, 0.90)"
    ).alias("p90_transactions"),
    F.expr(
        "percentile_approx(transaction_count, 0.95)"
    ).alias("p95_transactions"),
    F.expr(
        "percentile_approx(transaction_count, 0.99)"
    ).alias("p99_transactions")
).show(truncate=False)

print("\n=== CUSTOMERS WITH MORE THAN 70 TRANSACTIONS ===")

activity.filter(
    F.col("transaction_count") > 70
).orderBy(
    F.desc("transaction_count")
).show(20, truncate=False)

spark.stop()
