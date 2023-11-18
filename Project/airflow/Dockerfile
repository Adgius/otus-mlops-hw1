FROM python:3.9.0

RUN mkdir /airflow 
ENV AIRFLOW_HOME=~/airflow

COPY requirements.txt requirements.txt

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get -y install build-essential 

RUN pip install -U pip \
 && pip install -U setuptools\ 
 && pip install -r requirements.txt 

RUN airflow db init \
 && airflow db upgrade \
 && airflow users create \
   --username admin \
   --firstname Airflow \
   --lastname Administrator \
   --role Admin \
   --email admin@example.org \
   --password 12345 \
 && airflow scheduler \
 && airflow webserver