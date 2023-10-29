FROM python:3.10-slim-buster
# Install python packages
COPY requirements.txt . 
RUN pip install -r requirements.txt && mkdir -p /root/.aws
COPY /home/ubuntu/otus-mlops-hw1/5/.data/credentials /root/.aws/credentials