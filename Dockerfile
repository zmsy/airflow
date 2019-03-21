FROM ubuntu:18.04

WORKDIR /airflow

RUN sudo apt update -y \
    && apt install build-essential \
    python3 \
    python3-pip \
    libmysqlclient-dev \
    libssl-dev \
    libffi-dev \
    curl \
    vim



RUN airflow scheduler &
CMD airflow webserver -p 8080
