CREATE DATABASE IF NOT EXISTS financial_intelligence;

USE financial_intelligence;

CREATE EXTERNAL TABLE IF NOT EXISTS silver_transactions (
    raw_value STRING,
    topic STRING,
    partition_id INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    event_id STRING,
    transaction_id STRING,
    customer_id STRING,
    event_time TIMESTAMP,
    ingested_at TIMESTAMP,
    amount DOUBLE,
    currency STRING,
    merchant_category STRING,
    city STRING,
    channel STRING,
    device_id STRING,
    transaction_type STRING,
    batch_id INT
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/financial-intelligence/silver/transactions/';

CREATE EXTERNAL TABLE IF NOT EXISTS transaction_anomalies (
    raw_value STRING,
    topic STRING,
    partition_id INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    event_id STRING,
    transaction_id STRING,
    customer_id STRING,
    event_time TIMESTAMP,
    ingested_at TIMESTAMP,
    amount DOUBLE,
    currency STRING,
    merchant_category STRING,
    city STRING,
    channel STRING,
    device_id STRING,
    transaction_type STRING,
    batch_id INT,
    history_count BIGINT,
    historical_avg_amount DOUBLE,
    historical_stddev DOUBLE,
    high_value_flag BOOLEAN,
    unusual_for_customer_flag BOOLEAN,
    refund_review_flag BOOLEAN,
    anomaly_reason STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/financial-intelligence/analytics/anomalies_v3/';
