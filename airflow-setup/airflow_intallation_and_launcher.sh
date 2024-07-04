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

# clone the project very important before launching this executable
# git clone -b airflowetl https://github.com/ONOKANA8/SolarPanelsNasa.git
# It's necessary to define a file which contains the API key : $API_KEY=xxxx
# 

# browse to repository cloned
cd ~/SolarPanelsNasa 

# install env settings module and creating and activating airflowenv 
sudo apt install python3-venv
python3 -m venv airflowenv

# create virtual environment
source airflowenv/bin/activate


# browse to the airflow-setup folder
cd airflow-setup

# install airflow and requirements dependencies
pip3 install -r ./requirements.txt
pip3 install "apache-airflow[celery]"==2.9.1


# retrieve and set API key as airflow environment variable 
airflow variables set AIRFLOW_VAR_VISUALCROSSING_API_TOKEN $API_KEY

# launch airflow  webserver and scheduler in the background
./entrypoint.sh

# browse to  airflow folder created after airflow db initialized
mkdir ~/airflow/dags

# move or copy every airflow dags you want to dags folder created
cp ~/SolarPanelsNasa/data_france/france_data_pipeline_dag.py ~/airflow/dags
cp ~/SolarPanelsNasa/data_ivorycoast/ivorycoast_data_pipeline_dag.py ~/airflow/dags

