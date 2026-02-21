CREATE SCHEMA IF NOT EXISTS mart;

-- Витрина: Траты клиентов
CREATE TABLE IF NOT EXISTS mart.client_spend_report (
    client_name VARCHAR(100),
    total_spent DECIMAL(12, 2),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Спарк обычно перезаписывает таблицу или делает append.
-- Если мы хотим, чтобы Spark создавал таблицу сам (saveAsTable), этот SQL можно пропустить.
-- Но для строгости схемы лучше создать заранее.
