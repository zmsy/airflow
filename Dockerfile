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
    vim \
    git

# install all of the python reqs
COPY requirements.txt .
ENV AIRFLOW_GPL_UNIDECODE="yes"
RUN pip3 install -r requirements.txt

COPY . .

# RUN airflow scheduler &
# CMD airflow webserver -p 8080

CMD /bin/bash
