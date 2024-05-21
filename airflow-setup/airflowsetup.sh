#!/usr/bin/env bash

# get update and upgrade
# Install dependencies and tools you might use
apt-get update -yqq && \
apt-get upgrade -yqq && \
apt-get install -yqq --no-install-recommends \ 
wget \
libczmq-dev \
curl \
libssl-dev \
git \
inetutils-telnet \
bind9utils freetds-dev \
libkrb5-dev \
libsasl2-dev \
libffi-dev libpq-dev \
freetds-bin build-essential \
default-libmysqlclient-dev \
apt-utils \
rsync \
zip \
unzip \
gcc \
vim \
nano \
locales \
&& apt-get clean

# install airflow and requirements
pip3 install -r requirements.txt
pip3 install "apache-airflow[celery]"==2.9.1

