#!/usr/bin/env bash

# get update and upgrade
apt-get update && apt-get upgrade
apt-get install pip

# install airflow
pip install -r requirements.txt
pip install "airflow-apache[celery]"==2.9.1

