#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_PATH = (
    "hdfs://localhost:9000/"
    "financial-intelligence/million_run/silver/transactions/batch_id=0"
)

spark = (
    SparkSession.builder
    .appName("FinancialIntelligenceAnalytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Load validated Silver transactions from HDFS
df = spark.read.parquet(HDFS_PATH)
df.createOrReplaceTempView("transactions")

print("\n" + "=" * 65)
print("REAL-TIME FINANCIAL INTELLIGENCE — ANALYTICS")
print("=" * 65)

# 1. Overall financial KPIs
print("\n1. OVERALL TRANSACTION KPIs")

spark.sql("""
    SELECT
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT customer_id) AS unique_customers,
        ROUND(SUM(amount), 2) AS total_transaction_value,
        ROUND(AVG(amount), 2) AS average_transaction_amount,
        ROUND(MAX(amount), 2) AS maximum_transaction_amount,
        SUM(CASE WHEN transaction_type = 'purchase'
                 THEN 1 ELSE 0 END) AS purchase_count,
        SUM(CASE WHEN transaction_type = 'refund'
                 THEN 1 ELSE 0 END) AS refund_count
    FROM transactions
""").show(truncate=False)

# 2. City-wise activity
print("\n2. TOP 10 CITIES BY TRANSACTION VOLUME")

spark.sql("""
    SELECT
        city,
        COUNT(*) AS transaction_count,
        ROUND(SUM(amount), 2) AS total_amount
    FROM transactions
    GROUP BY city
    ORDER BY transaction_count DESC
    LIMIT 10
""").show(truncate=False)

# 3. Merchant-category analysis
print("\n3. MERCHANT CATEGORY PERFORMANCE")

spark.sql("""
    SELECT
        merchant_category,
        COUNT(*) AS transaction_count,
        ROUND(SUM(amount), 2) AS total_amount,
        ROUND(AVG(amount), 2) AS average_amount
    FROM transactions
    GROUP BY merchant_category
    ORDER BY total_amount DESC
""").show(truncate=False)

# 4. Purchase vs refund analysis
print("\n4. PURCHASE VS REFUND")

spark.sql("""
    SELECT
        transaction_type,
        COUNT(*) AS transaction_count,
        ROUND(SUM(amount), 2) AS total_amount,
        ROUND(AVG(amount), 2) AS average_amount
    FROM transactions
    GROUP BY transaction_type
    ORDER BY transaction_type
""").show(truncate=False)

# 5. Channel analysis
print("\n5. TRANSACTION CHANNEL ANALYSIS")

spark.sql("""
    SELECT
        channel,
        COUNT(*) AS transaction_count,
        ROUND(SUM(amount), 2) AS total_amount
    FROM transactions
    GROUP BY channel
    ORDER BY transaction_count DESC
""").show(truncate=False)

# 6. Daily transaction trend
print("\n6. DAILY TRANSACTION TREND — FIRST 15 DAYS")

spark.sql("""
    SELECT
        TO_DATE(event_time) AS transaction_date,
        COUNT(*) AS transaction_count,
        ROUND(SUM(amount), 2) AS total_amount
    FROM transactions
    GROUP BY TO_DATE(event_time)
    ORDER BY transaction_date
    LIMIT 15
""").show(15, truncate=False)

print("\nAnalytics completed successfully.")

spark.stop()
