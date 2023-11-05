echo -e "AIRFLOW_UID=$(id -u)" >> $HOME/otus-mlops-hw1/5/.env
echo -e "SSH_KEY=$(cat $HOME/.ssh/id_rsa.pub)" >> $HOME/otus-mlops-hw1/5/.env
echo -e "MLFLOW_URL=$(curl ifconfig.me)" >> $HOME/otus-mlops-hw1/5/.env
echo -e "MLFLOW_S3_ENDPOINT_URL=https://storage.yandexcloud.net" >> $HOME/otus-mlops-hw1/5/.env
echo -e "AWS_DEFAULT_REGION=ru-central1" >> $HOME/otus-mlops-hw1/5/.env
echo -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> $HOME/otus-mlops-hw1/5/.env
echo -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> $HOME/otus-mlops-hw1/5/.env
