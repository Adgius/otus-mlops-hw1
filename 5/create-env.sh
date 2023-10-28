echo -e "AIRFLOW_UID=$(id -u)" >> $HOME/otus-mlops-hw1/5/.env
echo -e "SSH_KEY=$(cat $HOME/.ssh/id_rsa.pub)" >> $HOME/otus-mlops-hw1/5/.env
echo -e "AWS_ACCESS_KEY_ID" >> $HOME/otus-mlops-hw1/5/.env
echo -e "AWS_SECRET_ACCESS_KEY" >> $HOME/otus-mlops-hw1/5/.env