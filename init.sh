#!/bin/bash
set -e

airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Signal that initialization is complete
touch /opt/airflow/airflow-webserver-ready

# Wait indefinitely
tail -f /dev/null