#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, concat_ws, lit, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "transactions"
HDFS_BASE = "hdfs://localhost:9000/financial-intelligence/recovery_run"
CHECKPOINT = f"{HDFS_BASE}/checkpoints/transactions"

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("FinancialIntelligenceTransactionStream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .select(
        col("value").cast("string").alias("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
)

def write_microbatch(batch_df, batch_id):
    batch_path = f"batch_id={batch_id}"

    raw_path = f"{HDFS_BASE}/bronze/transactions/{batch_path}"
    silver_path = f"{HDFS_BASE}/silver/transactions/{batch_path}"
    quarantine_path = f"{HDFS_BASE}/quarantine/transactions/{batch_path}"

    # Bronze: preserve raw Kafka values and source metadata.
    batch_df.select(
        "raw_value", "topic", "partition", "offset", "kafka_timestamp"
    ).write.mode("overwrite").parquet(raw_path)

    records = batch_df.withColumn(
        "record", from_json(col("raw_value"), schema)
    )

    for field in schema.fieldNames():
        records = records.withColumn(field, col(f"record.{field}"))

    records = (
        records
        .withColumn("event_time", to_timestamp("event_time"))
        .withColumn("ingested_at", to_timestamp("ingested_at"))
    )

    # Build validation reasons. Explicit null checks are important:
    # concat_ws skips null arguments.
    conditions = [
        when(
            col("event_id").isNull() | (length(col("event_id")) == 0),
            lit("missing event_id")
        ),
        when(
            col("transaction_id").isNull() | (length(col("transaction_id")) == 0),
            lit("missing transaction_id")
        ),
        when(
            col("customer_id").isNull() | (length(col("customer_id")) == 0),
            lit("missing customer_id")
        ),
        when(col("event_time").isNull(), lit("invalid event_time")),
        when(col("ingested_at").isNull(), lit("invalid ingested_at")),
        when(col("amount").isNull(), lit("missing or invalid amount")),
        when(col("amount") < 0, lit("negative amount")),
        when(col("currency") != "INR", lit("unsupported currency")),
        when(
            ~col("transaction_type").isin("purchase", "refund"),
            lit("invalid transaction_type")
        ),
    ]

    records = records.withColumn(
        "error_reason", concat_ws("; ", *conditions)
    )

    valid_batch = records.filter(col("error_reason") == "")
    invalid_batch = records.filter(col("error_reason") != "")

    # Silver: standardized valid records.
    valid_batch.drop("record", "error_reason").write \
        .mode("overwrite").parquet(silver_path)

    # Quarantine: preserve rejected records and their reasons.
    invalid_batch.drop("record").write \
        .mode("overwrite").parquet(quarantine_path)

    valid_count = valid_batch.count()
    invalid_count = invalid_batch.count()

    print(
        f"Completed batch {batch_id}: "
        f"{valid_count} valid, {invalid_count} invalid"
    )

query = (
    raw.writeStream
    .foreachBatch(write_microbatch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming transactions from Kafka. Press Ctrl+C to stop.")
query.awaitTermination()
