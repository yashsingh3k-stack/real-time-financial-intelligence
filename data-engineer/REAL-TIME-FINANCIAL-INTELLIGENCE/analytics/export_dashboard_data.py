import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_BASE = "hdfs://localhost:9000/financial-intelligence/million_run"

SILVER_PATH = (
    f"{HDFS_BASE}/silver/transactions/batch_id=0"
)

ANOMALY_BASE = f"{HDFS_BASE}/analytics/anomalies_v3"

OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "financial-intelligence",
    "data",
    "dashboard.json",
)

spark = (
    SparkSession.builder
    .appName("FinancialIntelligenceDashboardExport")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    transactions = spark.read.parquet(SILVER_PATH)

    summary = transactions.agg(
        F.count("*").alias("total_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("average_amount"),
        F.max("amount").alias("maximum_amount"),
    ).first()

    transaction_types = {
        row["transaction_type"]: row["count"]
        for row in transactions.groupBy("transaction_type")
        .count().collect()
    }

    cities = [
        {"name": row["city"], "count": row["count"]}
        for row in transactions.groupBy("city")
        .count().orderBy(F.desc("count")).collect()
    ]

    channels = [
        {"name": row["channel"], "count": row["count"]}
        for row in transactions.groupBy("channel")
        .count().orderBy(F.desc("count")).collect()
    ]

    anomaly_counts = {}

    for rule in [
        "high_value_transactions",
        "high_activity_customers",
        "customer_relative_high_amounts",
    ]:
        path = f"{ANOMALY_BASE}/{rule}"
        anomaly_counts[rule] = spark.read.parquet(path).count()

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Spark / HDFS Silver Parquet",
        "kpis": {
            "total_transactions": summary["total_transactions"],
            "unique_customers": summary["unique_customers"],
            "total_amount": float(summary["total_amount"] or 0),
            "average_amount": float(summary["average_amount"] or 0),
            "maximum_amount": float(summary["maximum_amount"] or 0),
        },
        "transaction_types": transaction_types,
        "cities": cities,
        "channels": channels,
        "anomalies": anomaly_counts,
    }

    output = os.path.abspath(OUTPUT_PATH)
    os.makedirs(os.path.dirname(output), exist_ok=True)

    with open(output, "w", encoding="utf-8") as file:
        json.dump(result, file, indent=2)

    print(f"\nDashboard data exported to: {output}")
    print(json.dumps(result, indent=2))

finally:
    spark.stop()
