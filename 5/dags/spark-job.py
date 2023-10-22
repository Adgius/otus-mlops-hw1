import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator


ssh_hook = SSHHook(ssh_conn_id='cluster_ssh_connection') 

MLFLOW_URL=os.getenv('MLFLOW_URL')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# To hide logs
Variable.set('AWS_ACCESS_KEY_ID', AWS_ACCESS_KEY_ID)
Variable.set('AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)

with DAG(
        dag_id='run_script',
        schedule_interval='0 6 * * *',
        start_date=datetime(2023, 9, 30),
        catchup=False,
        dagrun_timeout=timedelta(minutes=120),
        tags=['airflow-hw-4'],
        ) as dag:

    sftp_task = SFTPOperator(
                task_id='sftp_transfer',
                ssh_hook=ssh_hook,
                local_filepath=['/opt/airflow/data/clean-data.py', '/opt/airflow/data/run_pipeline.py', '/opt/airflow/data/mlflow-spark-1.27.0.jar'],
                remote_filepath=['/home/ubuntu/clean-data.py', '/home/ubuntu/run_pipeline.py', '/home/ubuntu/mlflow-spark-1.27.0.jar'],
                operation='put'
            )

    ssh_task1 = SSHOperator(
                task_id="installing_python_libs",
                command='pip install mlflow==1.27.0 findspark urllib3==1.25.8',
                ssh_hook=ssh_hook,
                cmd_timeout=None)
    
    ssh_task2 = SSHOperator(
                task_id="execute_ETL",
                command="/opt/conda/bin/python /home/ubuntu/clean-data.py {} {}".format(Variable.get("AWS_ACCESS_KEY_ID"), Variable.get("AWS_SECRET_ACCESS_KEY")), # Почему-то в системе стоят два питона
                ssh_hook=ssh_hook,
                get_pty=False,
                cmd_timeout=None)
    
    ssh_task3 = SSHOperator(
            task_id="train_model",
            command="spark-submit --jars /home/ubuntu/mlflow-spark-1.27.0.jar /home/ubuntu/run_pipeline.py -o {} -u {} -k {} -s {}".format('baseline', MLFLOW_URL, 
                                                                                                                                           Variable.get("AWS_ACCESS_KEY_ID"), Variable.get("AWS_SECRET_ACCESS_KEY")),
            ssh_hook=ssh_hook,
            get_pty=False,
            cmd_timeout=None)
    
    sftp_task >> ssh_task1 >> ssh_task2 >> ssh_task3
if __name__ == "__main__":
    dag.cli()
