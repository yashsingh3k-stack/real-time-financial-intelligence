#!/usr/bin/env python3

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    avg,
    stddev_pop,
    abs as spark_abs,
    when,
    count,
    round as spark_round
)

spark = (
    SparkSession.builder
    .appName("FinancialAnomalyDetection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/financial-intelligence"
silver_path = base + "/silver/transactions/"
output_path = base + "/analytics/anomalies/"

df = spark.read.parquet(silver_path)

# Exclude records without the fields needed for analysis.
df = df.filter(
    col("customer_id").isNotNull()
    & col("amount").isNotNull()
    & col("event_time").isNotNull()
    & (col("amount") >= 0)
)

# Calculate each customer's historical baseline.
customer_window = Window.partitionBy("customer_id")

df = (
    df.withColumn(
        "customer_avg_amount",
        avg("amount").over(customer_window)
    )
    .withColumn(
        "customer_stddev",
        stddev_pop("amount").over(customer_window)
    )
)

# Apply transparent, explainable review rules.
df = df.withColumn(
    "anomaly_reason",
    when(
        col("amount") >= 40000,
        "HIGH_VALUE_TRANSACTION"
    )
    .when(
        (col("customer_stddev") > 0)
        & (
            col("amount")
            > col("customer_avg_amount") + 2 * col("customer_stddev")
        ),
        "UNUSUALLY_HIGH_FOR_CUSTOMER"
    )
    .when(
        col("transaction_type") == "refund",
        "REFUND_REVIEW"
    )
)

anomalies = df.filter(col("anomaly_reason").isNotNull())

# Display the results.
print("\n========== ANOMALY DETECTION SUMMARY ==========")
print("Transactions analyzed:", df.count())
print("Transactions flagged for review:", anomalies.count())

print("\n========== FLAGGED TRANSACTIONS ==========")
anomalies.select(
    "transaction_id",
    "customer_id",
    "event_time",
    "amount",
    "transaction_type",
    spark_round("customer_avg_amount", 2).alias("customer_avg_amount"),
    "anomaly_reason"
).orderBy(
    col("amount").desc()
).show(50, truncate=False)

# Save results as Parquet in HDFS.
(
    anomalies.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Anomaly results saved to:", output_path)

spark.stop()
