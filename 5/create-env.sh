echo -e "AIRFLOW_UID=$(id -u)" >> $HOME/otus-mlops-hw1/5/.env
echo -e "SSH_KEY=$(cat $HOME/.ssh/id_rsa.pub)" >> $HOME/otus-mlops-hw1/5/.env
echo -e "MLFLOW_URL=$(curl ifconfig.me)" >> $HOME/otus-mlops-hw1/5/.env

echo -e [default] >> $HOME/otus-mlops-hw1/5/credentials
echo -e endpoint_url='https://storage.yandexcloud.net' >> $HOME/otus-mlops-hw1/5/credentials
echo -e region_name = 'ru-central1' >> $HOME/otus-mlops-hw1/5/credentials
echo -e aws_access_key_id = $AWS_ACCESS_KEY_ID >> $HOME/otus-mlops-hw1/5/credentials
echo -e aws_secret_access_key = $AWS_SECRET_ACCESS_KEY >> $HOME/otus-mlops-hw1/5/credentials