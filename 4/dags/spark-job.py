
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator


ssh_hook = SSHHook(ssh_conn_id='cluster_ssh_connection') 

with DAG(
        dag_id='spark_ETL',
        schedule_interval='@once',
        start_date=datetime(2023, 9, 24),
        catchup=False,
        dagrun_timeout=timedelta(minutes=120),
        tags=['airflow-hw-4'],
        ) as dag:

    sftp_task = SFTPOperator(
                    task_id='sftp_transfer',
                    ssh_hook=ssh_hook,
                    local_filepath='/opt/airflow/data/clean-data.py',
                    remote_filepath='/home/ubuntu/clean-data.py',
                    operation='put'
                )
if __name__ == "__main__":
    dag.cli()
