import requests
import json
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import models
from airflow import settings


logger = logging.getLogger(__name__)

def create_token():
    params = {'yandexPassportOauthToken': Variable.get('yc_token')}
    response = requests.post('https://iam.api.cloud.yandex.net/iam/v1/tokens', params=params)                                                   
    decode_response = response.content.decode('UTF-8')
    text = json.loads(decode_response) 
    iam_token = text.get('iamToken')
    logger.info(response)
    expires_iam_token = text.get('expiresAt')
    return iam_token

def get_folder_id(**kwargs):
    ti = kwargs['ti']
    response = requests.get("https://resource-manager.api.cloud.yandex.net/resource-manager/v1/clouds",  
        headers={"Authorization": f"Bearer {ti.xcom_pull('get_token')}"})
    logger.info(response)
    clouds = json.loads(response.content)
    cloud_id = clouds['clouds'][0]['id']
    folders = json.loads(requests.get("https://resource-manager.api.cloud.yandex.net/resource-manager/v1/folders",  
        headers={"Authorization": f"Bearer {ti.xcom_pull('get_token')}"},
        params={'cloud_id': cloud_id}).content)
    folder_id = folders['folders'][0]['id']
    return folder_id



def create_cluster(**kwargs):
    ti = kwargs['ti']
    body = {
    "folderId": ti.xcom_pull('get_folder_id'), 
    "name": "my-dataproc",
    "configSpec": {
        "versionId": "2.0",
        "hadoop": {
        "sshPublicKeys": [
            Variable.get('ssh-key')
            ]
        },
        "subclustersSpec": [
        {
            "role": "MASTERNODE",
            "resources": {
            "resourcePresetId": "s3-c2-m8",
            "diskTypeId": "network-ssd",
            "diskSize": "42949672960"
            },
            "subnetId": "e2l9t3irqejhg9s3trbp",  # Auto-created default subnet for zone ru-central1-b
            "hostsCount": "1",
            "assignPublicIp": True
        },
        {
            "role": "DATANODE",
            "resources": {
            "resourcePresetId": "s3-c4-m16",
            "diskTypeId": "network-ssd",
            "diskSize": "137438953472"
            },
            "subnetId": "e2l9t3irqejhg9s3trbp",  # Auto-created default subnet for zone ru-central1-b
            "hostsCount": "1",
            "assignPublicIp": False
        }
        ]
    },
    "zoneId": "ru-central1-b",
    "serviceAccountId": "ajeu90dsieg85rfn58i2",  # your service acc
    "uiProxy": True,
    "securityGroupIds": [
        "enp71tcdhpj838buvadk"  # your sec group
    ],
    "deletionProtection": False
    }
    cluster_info = json.loads(requests.post("https://dataproc.api.cloud.yandex.net/dataproc/v1/clusters", 
       json=body, 
       headers={"Authorization": f"Bearer {ti.xcom_pull('get_token')}"}).content)
    cluster_id = cluster_info['metadata']['clusterId']
    return cluster_id

def get_masternode_ip(**kwargs):
    ti = kwargs['ti']
    cluster_hosts = json.loads(requests.get(f"https://dataproc.api.cloud.yandex.net/dataproc/v1/clusters/{ti.xcom_pull('create_cluster')}/hosts", 
       headers={"Authorization": f"Bearer {ti.xcom_pull('get_token')}"}
       ).content)
    logger.info(cluster_hosts)
    for h in cluster_hosts['hosts']:
        if h['role'] == 'MASTERNODE':
            masternode_id = h['computeInstanceId']
            break
    masternode_info = json.loads(requests.get(f"https://compute.api.cloud.yandex.net/compute/v1/instances/{masternode_id}", 
                                              headers={"Authorization": f"Bearer {ti.xcom_pull('get_token')}"}).content)
    masternode_ip = masternode_info['networkInterfaces'][0]['primaryV4Address']['oneToOneNat']['address']
    print(masternode_ip)
    return masternode_ip

def create_ssh_connection(**kwargs):
    ti = kwargs['ti']
    conn = models.Connection(
        conn_id='cluster_ssh_connection',
        conn_type='ssh',
        host=ti.xcom_pull('get_masternode_ip'),
        login='ubuntu',
        port=22,
        extra={
            'key_file': '/opt/airflow/.ssh/id_rsa',
            "conn_timeout": "10",
            "compress": "false",
            "look_for_keys": "false",
            "allow_host_key_change": "false",
            "disabled_algorithms": {"pubkeys": ["rsa-sha2-256", "rsa-sha2-512"]},
            "ciphers": ["aes128-ctr", "aes192-ctr", "aes256-ctr"]
        }
    )

    session = settings.Session()
    session.add(conn)
    session.commit()

with DAG(
    dag_id='spark_ETL',
    schedule_interval='@once',
    start_date=datetime(2023, 9, 24),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=['airflow-hw-4'],
) as dag:
    get_token = PythonOperator(
        task_id='get_token',
        python_callable=create_token
    )
    get_folder_id = PythonOperator(
        task_id='get_folder_id',
        python_callable=get_folder_id
    ) 
    create_cluster = PythonOperator(
        task_id='create_cluster',
        python_callable=create_cluster
    )  
    await_cluster = BashOperator(task_id="await_cluster",
                                 bash_command="sleep 10m")
    get_masternode_ip = PythonOperator(
        task_id='get_masternode_ip',
        python_callable=get_masternode_ip
    )
    create_ssh_connection = PythonOperator(
        task_id='create_ssh_connection',
        python_callable=create_ssh_connection
    ) 
    ssh_hook = SSHHook(ssh_conn_id='cluster_ssh_connection') 
    sftp_task = SFTPOperator(
        task_id='sftp_transfer',
        ssh_hook=ssh_hook,
        local_filepath='/opt/airflow/data/clean-data.py',
        remote_filepath='/home/ubuntu/clean-data.py',
        operation='put'
    )
    get_token >> get_folder_id >> create_cluster >> await_cluster >> get_masternode_ip >> create_ssh_connection >> sftp_task


if __name__ == "__main__":
    dag.cli()