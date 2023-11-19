FROM python:3.9.0

RUN mkdir /opt/airflow/
ENV AIRFLOW_HOME=/opt/airflow/

COPY requirements.txt requirements.txt
COPY start_airflow.sh /opt/airflow/start_airflow_db.sh

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get -y install build-essential 

RUN pip install -U pip \
 && pip install -U setuptools\ 
 && pip install -r requirements.txt 

RUN chmod +x /opt/airflow/start_airflow_db.sh

ENTRYPOINT ["/opt/airflow/start_airflow_db.sh"]