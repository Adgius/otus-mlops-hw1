
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

    ssh_task1 = SSHOperator(
                task_id="execute_findspark",
                command='pip install findspark',
                ssh_hook=ssh_hook)
    
    ssh_task2 = SSHOperator(
                task_id="execute",
                command=f'python /home/ubuntu/clean-data.py {aws_access_key_id} {aws_secret_access_key}',
                ssh_hook=ssh_hook,
                get_pty=True)
    
    execute_script = BashOperator(
        task_id="execute_script",
        bash_command=f"ssh ubuntu@{{ ti.xcom_pull(\'get_masternode_ip\') }} 'python' '/home/ubuntu/clean-data.py' '{aws_access_key_id}' {aws_secret_access_key}'",
    )
    
    sftp_task >> ssh_task1 >> execute_script
if __name__ == "__main__":
    dag.cli()
