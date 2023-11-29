import logging
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from src.pipeline.reviews import run_reviews
from src.pipeline.rating import run_rating

with DAG(
    dag_id='update_data',
    schedule_interval='@once',
    start_date=datetime(2023, 11, 29),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120)
) as dag:
    update_reviews = PythonOperator(
        task_id='get_reviews',
        python_callable=run_reviews,
        provide_context=True
    )
    update_rating = PythonOperator(
        task_id='get_rating',
        python_callable=run_rating,
        provide_context=True
    )

    update_rating >> update_reviews


if __name__ == "__main__":
    dag.cli()