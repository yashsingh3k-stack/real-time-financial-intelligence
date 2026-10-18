from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerifyTransactions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/financial-intelligence"

silver = spark.read.parquet(base + "/silver/transactions/")
quarantine = spark.read.parquet(base + "/quarantine/transactions/")

print("Valid transactions:", silver.count())
print("Invalid transactions:", quarantine.count())

print("\n--- Valid transaction samples ---")
silver.select(
    "transaction_id",
    "customer_id",
    "amount",
    "currency",
    "transaction_type"
).show(10, truncate=False)

print("\n--- Quarantined transaction samples ---")
quarantine.select(
    "transaction_id",
    "error_reason"
).show(10, truncate=False)

spark.stop()
