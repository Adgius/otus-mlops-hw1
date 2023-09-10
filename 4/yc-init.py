from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id='airflow-test',
    schedule_interval='@once',
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    tags=['airflow-test'],
) as dag:
    download = BashOperator(
        task_id='yc_download',
        bash_command='curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash -s -- -h'
    )
    create_profile = BashOperator(
        task_id='yc_init',
        bash_command='yc init'
    )
    choice_new = BashOperator(
        task_id='creat_new_profile',
        bash_command='2',
    )
    create_name = BashOperator(
        task_id='create_name',
        bash_command='test-airflow',
    )
    set_oauth = BashOperator(
        task_id='get_oauth',
        bash_command=Variable.get('yc_token'),
    )
    choice_folder = BashOperator(
        task_id='choice_folder',
        bash_command='1',
    )
    compute_zone = BashOperator(
        task_id='compute_zone',
        bash_command='Y',
    )
    set_default_zone = BashOperator(
        task_id='set_default_zone',
        bash_command='2',
    )
    download >> create_profile >> choice_new >> create_name >> set_oauth >> choice_folder >> compute_zone >> set_default_zone

if __name__ == "__main__":
    dag.cli()