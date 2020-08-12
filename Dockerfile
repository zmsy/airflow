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
ENV AIRFLOW_GPL_UNIDECODE="yes"
RUN pip install pipenv
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv install --deploy

COPY . .

RUN chmod +x entrypoint.sh
ENTRYPOINT [ "/bin/bash", "entrypoint.sh" ]
