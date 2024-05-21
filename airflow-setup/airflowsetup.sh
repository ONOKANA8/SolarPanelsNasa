#!/usr/bin/env bash

# get update and upgrade
sudo apt-get update && sudo apt-get upgrade
sudo apt-get install python3-pip

# install venv 
sudo apt install python3.12-venv

# create airflow environment
python3 -m venv airflowenv

# activate airflowenv
source airflowenv/bin/activate

# install airflow
pip install -r requirements.txt
pip install "apache-airflow[celery]"==2.9.1

