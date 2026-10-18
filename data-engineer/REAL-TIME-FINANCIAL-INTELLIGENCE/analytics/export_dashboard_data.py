import json
import os
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_BASE = "hdfs://localhost:9000/financial-intelligence/million_run"
SILVER_PATH = f"{HDFS_BASE}/silver/transactions/batch_id=0"
ANOMALY_BASE = f"{HDFS_BASE}/analytics/anomalies_v3"

OUTPUT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "financial-intelligence",
        "data",
        "dashboard.json",
    )
)

ANOMALY_RULES = [
    "high_value_transactions",
    "high_activity_customers",
    "customer_relative_high_amounts",
]


def main():
    spark = (
        SparkSession.builder
        .appName("FinancialIntelligenceDashboardExport")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    temp_path = None

    try:
        transactions = spark.read.parquet(SILVER_PATH)

        summary = transactions.agg(
            F.count("*").alias("total_transactions"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("average_amount"),
            F.max("amount").alias("maximum_amount"),
        ).first()

        if not summary or summary["total_transactions"] <= 0:
            raise ValueError("Silver dataset contains no transactions.")

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
        for rule in ANOMALY_RULES:
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

        output_dir = os.path.dirname(OUTPUT_PATH)
        os.makedirs(output_dir, exist_ok=True)

        # Write in the destination directory so os.replace is atomic.
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output_dir,
            prefix=".dashboard-",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = temp_file.name
            json.dump(result, temp_file, indent=2, allow_nan=False)
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())

        # Validate the complete file before replacing the current snapshot.
        with open(temp_path, "r", encoding="utf-8") as temp_file:
            validated = json.load(temp_file)

        if validated.get("kpis", {}).get("total_transactions", 0) <= 0:
            raise ValueError("Generated dashboard JSON failed validation.")

        os.replace(temp_path, OUTPUT_PATH)
        temp_path = None

        print(f"Dashboard data exported successfully: {OUTPUT_PATH}")
        print(json.dumps(result, indent=2))

    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        spark.stop()


if __name__ == "__main__":
    main()
