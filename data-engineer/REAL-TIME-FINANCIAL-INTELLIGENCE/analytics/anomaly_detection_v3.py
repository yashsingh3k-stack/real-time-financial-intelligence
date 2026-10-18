#!/usr/bin/env python3

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    avg,
    stddev_pop,
    count,
    when,
    lit,
    concat,
    round as spark_round
)

spark = (
    SparkSession.builder
    .appName("FinancialAnomalyDetectionV2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/financial-intelligence"
silver_path = base + "/silver/transactions/"
output_path = base + "/analytics/anomalies_v3/"

df = spark.read.parquet(silver_path)

# Keep records with usable analysis fields.
df = df.filter(
    col("customer_id").isNotNull()
    & col("transaction_id").isNotNull()
    & col("event_time").isNotNull()
    & col("amount").isNotNull()
    & (col("amount") >= 0)
)

# Use only earlier transactions for each customer's baseline.
history = (
    Window.partitionBy("customer_id")
    .orderBy(col("event_time").asc(), col("transaction_id").asc())
    .rowsBetween(Window.unboundedPreceding, -1)
)

df = (
    df.withColumn("history_count", count("amount").over(history))
    .withColumn("historical_avg_amount", avg("amount").over(history))
    .withColumn("historical_stddev", stddev_pop("amount").over(history))
)

# Independent review indicators.
df = (
    df.withColumn(
        "high_value_flag",
        col("amount") >= 40000
    )
    .withColumn(
        "unusual_for_customer_flag",
        (col("history_count") >= 3)
        & (col("historical_stddev") > 0)
        & (
            col("amount")
            > col("historical_avg_amount") + 2 * col("historical_stddev")
        )
    )
    .withColumn(
        "refund_review_flag",
        col("transaction_type") == "refund"
    )
)

# Build a list of reasons without hiding other flags.
df = df.withColumn(
    "anomaly_reason",
    when(col("high_value_flag"), lit("HIGH_VALUE_TRANSACTION"))
)

df = df.withColumn(
    "anomaly_reason",
    when(
        col("unusual_for_customer_flag"),
        when(
            col("anomaly_reason").isNotNull(),
            concat(col("anomaly_reason"), lit(",UNUSUAL_FOR_CUSTOMER"))
        ).otherwise(lit("UNUSUAL_FOR_CUSTOMER"))
    ).otherwise(col("anomaly_reason"))
)

df = df.withColumn(
    "anomaly_reason",
    when(
        col("refund_review_flag"),
        when(
            col("anomaly_reason").isNotNull(),
            concat(col("anomaly_reason"), lit(",REFUND_REVIEW"))
        ).otherwise(lit("REFUND_REVIEW"))
    ).otherwise(col("anomaly_reason"))
)

# Save records with at least one review indicator.
anomalies = df.filter(
    col("high_value_flag")
    | col("unusual_for_customer_flag")
    | col("refund_review_flag")
)

print("\n========== V2 SUMMARY ==========")
print("Transactions analyzed:", df.count())
print("Records flagged:", anomalies.count())

print("\n========== FLAG REASON COUNTS ==========")
(
    anomalies.selectExpr(
        "explode(split(anomaly_reason, ',')) AS reason",
        "amount"
    )
    .groupBy("reason")
    .agg(count("*").alias("records"))
    .orderBy("reason")
    .show(truncate=False)
)

print("\n========== FLAGGED TRANSACTIONS ==========")
anomalies.select(
    "transaction_id",
    "customer_id",
    "event_time",
    "amount",
    "transaction_type",
    "history_count",
    spark_round("historical_avg_amount", 2).alias("historical_avg_amount"),
    "anomaly_reason"
).orderBy(col("amount").desc()).show(50, truncate=False)

# Separate output path preserves Version 1 results.
anomalies.write.mode("overwrite").parquet(output_path)

print("V2 results saved to:", output_path)

spark.stop()


