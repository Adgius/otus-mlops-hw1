
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


ssh_hook = SSHHook(ssh_conn_id='cluster_ssh_connection') 

def run_remote_script():
    aws_access_key_id = Variable.get('aws_access_key_id')
    aws_secret_access_key = Variable.get('aws_secret_access_key')
    ssh_hook.exec_ssh_client_command(command=f'python /home/ubuntu/clean-data.py {aws_access_key_id} {aws_secret_access_key}',
                                     get_pty=True,
                                     environment=None)

with DAG(
        dag_id='run_script',
        schedule_interval='*/20 * * * *',
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


    ssh_task = PythonOperator(
        task_id='run_remote_script',
        python_callable=run_remote_script,
        dag=dag,
    )
    sftp_task >> ssh_task
if __name__ == "__main__":
    dag.cli()
