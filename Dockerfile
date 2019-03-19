FROM ubuntu:16.04

RUN sudo apt update -y \
    && apt install build-essential \
    python3 \
    python3-pip \
    libmysqlclient-dev \
    libssl-dev \
    libffi-dev \
    curl \
    vim
