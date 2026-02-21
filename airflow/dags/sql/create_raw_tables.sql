-- Создаем схемы, если их нет (лучше делать это здесь, уже подключившись к нужной БД)
CREATE SCHEMA IF NOT EXISTS raw;

-- Таблица клиентов
CREATE TABLE IF NOT EXISTS raw.clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    registration_date DATE
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS raw.orders (
    id SERIAL PRIMARY KEY,
    client_id INT,
    amount DECIMAL(10, 2),
    order_date DATE,
    FOREIGN KEY (client_id) REFERENCES raw.clients(id)
);

-- Очищаем таблицы перед новой вставкой (для идемпотентности в учебном примере)
-- В реальном DWH тут был бы append, но для тестов проще truncate
TRUNCATE TABLE raw.orders RESTART IDENTITY CASCADE;
TRUNCATE TABLE raw.clients RESTART IDENTITY CASCADE;