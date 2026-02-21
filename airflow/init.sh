#!/bin/bash

# Остановка при любой ошибке
set -e

echo "Starting project initialization..."

# 1. Создание .env файла с текущим UID
# Это решает проблему Permission Denied для логов
if [ ! -f .env ]; then
    echo "Creating .env file..."
    echo "AIRFLOW_UID=$(id -u)" > .env
    # Добавляем другие переменные, если нужно (например, версию Airflow)
    echo "AIRFLOW_PROJ_DIR=." >> .env
    echo ".env created with UID=$(id -u)"
else
    echo ".env file already exists. Skipping."
fi

if [ ! -f apps/postgresql-42.7.3.jar ]; then
    echo "Downloading Postgres JDBC Driver..."
    curl -o ../spark/apps/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
    echo "Driver downloaded."
fi

echo "Initialization complete!"
echo "You can now run: docker compose up -d"