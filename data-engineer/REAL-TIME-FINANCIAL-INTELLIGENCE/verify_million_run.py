from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyMillionRun") \
    .getOrCreate()

base = "hdfs://localhost:9000/financial-intelligence/million_run"

for layer in ["silver", "quarantine", "bronze"]:
    path = f"{base}/{layer}/transactions/batch_id=0"
    try:
        df = spark.read.parquet(path)
        print(f"{layer.upper()} count: {df.count():,}")
        df.printSchema()
    except Exception as e:
        print(f"{layer.upper()} read failed: {e}")

spark.stop()
