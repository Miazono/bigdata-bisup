#!/bin/bash


set -e

echo "Starting..."

if [ ! -f .env ]; then
    echo "Creating .env file..."
    echo "AIRFLOW_UID=$(id -u)" > .env
    echo "AIRFLOW_PROJ_DIR=." >> .env
    echo ".env created with UID=$(id -u)"
else
    echo ".env file already exists. Skipping."
fi

echo "Initialization complete!"
