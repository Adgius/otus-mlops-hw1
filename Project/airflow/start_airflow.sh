#!/bin/bash

airflow db init
airflow db upgrade

airflow users create \
   --username airflow \
   --firstname Airflow \
   --lastname Administrator \
   --role Admin \
   --email admin@example.org \
   --password 12345

airflow scheduler & airflow webserver