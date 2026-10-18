#!/usr/bin/env python3

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS_BASE = (
    "hdfs://localhost:9000/"
    "financial-intelligence/million_run"
)

SILVER_PATH = f"{HDFS_BASE}/silver/transactions/batch_id=0"

# New output directory: preserves anomalies_v2
OUTPUT_BASE = f"{HDFS_BASE}/analytics/anomalies_v3"

spark = (
    SparkSession.builder
    .appName("FinancialAnomalyDetectionV3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(SILVER_PATH)

# Rule 1: High-value transactions (>= ₹45,000)
high_value = (
    df.filter(F.col("amount") >= 45000)
    .withColumn(
        "anomaly_type",
        F.lit("HIGH_TRANSACTION_AMOUNT")
    )
)

# Rule 2: High customer activity (>= 68 transactions)
customer_activity = (
    df.groupBy("customer_id")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_amount")
    )
)

high_activity = (
    customer_activity
    .filter(F.col("transaction_count") >= 68)
    .withColumn(
        "anomaly_type",
        F.lit("HIGH_CUSTOMER_ACTIVITY")
    )
)

# Rule 3: Customer-relative high amount
# Flag amounts above the customer's average + 2 standard deviations.
customer_stats = (
    df.groupBy("customer_id")
    .agg(
        F.count("*").alias("customer_transaction_count"),
        F.avg("amount").alias("customer_average_amount"),
        F.stddev_pop("amount").alias("customer_amount_stddev")
    )
)

customer_relative = (
    df.join(customer_stats, on="customer_id", how="inner")
    .filter(
        (F.col("customer_transaction_count") >= 20)
        & (
            F.col("amount")
            > (
                F.col("customer_average_amount")
                + 2 * F.col("customer_amount_stddev")
            )
        )
    )
    .withColumn(
        "anomaly_type",
        F.lit("CUSTOMER_RELATIVE_HIGH_AMOUNT")
    )
    .select(
        "transaction_id",
        "customer_id",
        "amount",
        "customer_average_amount",
        "customer_amount_stddev",
        "customer_transaction_count",
        "city",
        "transaction_type",
        "anomaly_type"
    )
)

# Print summary counts
print("\n=== RULE 1: HIGH-VALUE TRANSACTIONS ===")
high_value.agg(
    F.count("*").alias("flagged_transactions"),
    F.round(F.max("amount"), 2).alias("maximum_amount")
).show(truncate=False)

print("\n=== RULE 2: HIGH-ACTIVITY CUSTOMERS ===")
high_activity.agg(
    F.count("*").alias("flagged_customers"),
    F.max("transaction_count").alias("maximum_transaction_count")
).show(truncate=False)

print("\n=== RULE 3: CUSTOMER-RELATIVE HIGH AMOUNTS ===")
customer_relative.agg(
    F.count("*").alias("flagged_transactions"),
    F.round(F.max("amount"), 2).alias("maximum_flagged_amount")
).show(truncate=False)

customer_relative.orderBy(
    F.desc("amount")
).show(20, truncate=False)

# Write to a new location, preserving anomalies_v2.
high_value.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/high_value_transactions"
)

high_activity.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/high_activity_customers"
)

customer_relative.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/customer_relative_high_amounts"
)

print("\nSaved results under:")
print(OUTPUT_BASE)

spark.stop()
