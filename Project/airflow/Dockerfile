FROM python:3.9.0

RUN mkdir /opt/airflow/
ENV AIRFLOW_HOME=/opt/airflow/

COPY requirements.txt requirements.txt
COPY start_airflow.sh /opt/airflow/start_airflow_db.sh

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get -y install build-essential 

RUN apt-get install dos2unix && dos2unix /opt/airflow/start_airflow_db.sh

RUN pip install -U pip \
 && pip install -U setuptools\ 
 && pip install -r requirements.txt \
 && pip install torch --index-url https://download.pytorch.org/whl/cpu


RUN chmod +x /opt/airflow/start_airflow_db.sh

CMD ["bash", "/opt/airflow/start_airflow_db.sh"]