#!/usr/bin/env bash

# Initiliase the metastore
airflow db init

# Create user
# -u: --username; -p: --password; -r: --role; -e: --email; -f: --firstname; -l: --lastname
airflow users create \
        -u admin -p admin password \
        -r Admin \
        -e adminemail@mail.com \
        -f admin  \
        -l admin


# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in foreground (for docker logs)
airflow webserver --port 8080 

