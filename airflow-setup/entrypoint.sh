#!/usr/bin/env bash

# Initiliase the metastore
airflow db init

# Create user
# -u: --username; -p: --password; -r: --role; -e: --email; -f: --firstname; -l: --lastname
airflow users create \
        -u donatello -p jtm \
        -r Admin \
        -e donatien.konan.pro@gmail.com \
        -f donatien  \
        -l konan


# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in foreground (for docker logs)
airflow webserver --port 8080 

