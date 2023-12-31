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

airflow connections add 'postgres' --conn-uri ${AIRFLOW_CONN_REVIEWS_DB}

airflow scheduler & airflow webserver