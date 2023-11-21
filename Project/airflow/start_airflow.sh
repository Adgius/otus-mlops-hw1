#!/bin/bash

airflow db migrate
airflow db upgrade

airflow users create \
   --username airflow \
   --firstname Airflow \
   --lastname Administrator \
   --role Admin \
   --email admin@example.org \
   --password 12345

airflow connections add 'postgres' \
    --conn-uri 'postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@mydb:5432/public'

airflow scheduler & airflow webserver