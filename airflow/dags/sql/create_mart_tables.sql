CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.client_spend_report (
    client_name VARCHAR(100),
    total_spent DECIMAL(12, 2),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

