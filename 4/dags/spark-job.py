from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator


ssh_hook = SSHHook(ssh_conn_id='cluster_ssh_connection') 
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

with DAG(
        dag_id='run_script',
        schedule_interval='0 7 * * *',
        start_date=datetime(2023, 9, 30),
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

    ssh_task1 = SSHOperator(
                task_id="execute_findspark",
                command='pip install findspark',
                ssh_hook=ssh_hook)
    
    ssh_task2 = SSHOperator(
                task_id="execute_script",
                command="/opt/conda/bin/python /home/ubuntu/clean-data.py {} {}".format(aws_access_key_id, aws_secret_access_key), # Почему-то в системе стоят два питона
                ssh_hook=ssh_hook,
                get_pty=False)
    
    sftp_task >> ssh_task1 >> ssh_task2
if __name__ == "__main__":
    dag.cli()
