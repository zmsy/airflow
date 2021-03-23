FROM python:3.7-slim

# set as non-interactive debian frontend
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

WORKDIR /airflow
ENV AIRFLOW_HOME /airflow

RUN apt-get update -y
RUN apt-get upgrade -y
RUN apt-get install -y build-essential \
    libssl-dev \
    libffi-dev \
    curl \
    jq

# install all of the python reqs
ENV AIRFLOW_GPL_UNIDECODE="yes"
COPY Pipfile.lock .

# export Pipfile.lock to requirements.txt
RUN jq -r '.default | to_entries[] | .key + .value.version' Pipfile.lock > requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

RUN chmod +x entrypoint.sh
ENTRYPOINT [ "/bin/bash", "entrypoint.sh" ]
