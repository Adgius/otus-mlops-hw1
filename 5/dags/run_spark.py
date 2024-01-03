import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator


ssh_hook = SSHHook(ssh_conn_id='cluster_ssh_connection') 

MLFLOW_URL = os.getenv('MLFLOW_URL')
MLFLOW_S3_ENDPOINT_URL = os.getenv('MLFLOW_S3_ENDPOINT_URL')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# To hide logs
Variable.set('AWS_ACCESS_KEY_ID', AWS_ACCESS_KEY_ID)
Variable.set('AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)

with DAG(
        dag_id='run_script',
        schedule_interval='@once',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=120),
        tags=['airflow-hw-5'],
        ) as dag:

    sftp_task = SFTPOperator(
                task_id='sftp_transfer',
                ssh_hook=ssh_hook,
                local_filepath=['/opt/airflow/data/test.py', '/opt/airflow/data/pyspark_env.sh', '/opt/airflow/data/run_pipeline.py', '/opt/airflow/data/mlflow-spark-1.27.0.jar'],
                remote_filepath=['/home/ubuntu/test.py', '/home/ubuntu/pyspark_env.sh', '/home/ubuntu/run_pipeline.py', '/home/ubuntu/mlflow-spark-1.27.0.jar'],
                operation='put',
                create_intermediate_dirs=True
            )
    
    ssh_task1 = SSHOperator(
                task_id="install_python_libs",
                command="bash /home/ubuntu/pyspark_env.sh ",
                ssh_hook=ssh_hook,
                cmd_timeout=None)
    
    ssh_task2 = SSHOperator(
                task_id="pack_python_libs",
                command="source /home/ubuntu/pyspark_venv/bin/activate; venv-pack -o pyspark_venv.tar.gz",
                ssh_hook=ssh_hook,
                cmd_timeout=None)

    ssh_task3 = SSHOperator(
            task_id="train_model",
            command='bash -l -c "spark-submit --archives pyspark_venv.tar.gz#environment \
            --master yarn \
            --deploy-mode cluster \
            --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
            --jars mlflow-spark-1.27.0.jar\
             run_pipeline.py -o {} -u {} -k {} -s {} -r {} -e {}"'.format('baseline', 
                                                                                     MLFLOW_URL, 
                                                                                     Variable.get("AWS_ACCESS_KEY_ID"), 
                                                                                     Variable.get("AWS_SECRET_ACCESS_KEY"),
                                                                                     AWS_DEFAULT_REGION,
                                                                                     MLFLOW_S3_ENDPOINT_URL),
            ssh_hook=ssh_hook,
            cmd_timeout=None)
    
    sftp_task >> ssh_task1 >> ssh_task2 >>  ssh_task3
if __name__ == "__main__":
    dag.cli()
