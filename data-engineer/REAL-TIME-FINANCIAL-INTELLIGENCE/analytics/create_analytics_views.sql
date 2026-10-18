USE financial_intelligence;

CREATE OR REPLACE VIEW city_transaction_summary AS
SELECT
    city,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS average_amount
FROM silver_transactions
GROUP BY city;

CREATE OR REPLACE VIEW transaction_type_summary AS
SELECT
    transaction_type,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS average_amount
FROM silver_transactions
GROUP BY transaction_type;

CREATE OR REPLACE VIEW anomaly_reason_summary AS
SELECT
    anomaly_reason,
    COUNT(*) AS flagged_records,
    ROUND(SUM(amount), 2) AS total_amount
FROM transaction_anomalies
GROUP BY anomaly_reason;
