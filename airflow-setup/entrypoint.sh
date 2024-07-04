 #!/usr/bin/env bash

# Initiliase the metastore
airflow db init

# Create user
# -u: --username; -p: --password; -r: --role; -e: --email; -f: --firstname; -l: --lastname
airflow users create \
        -u xxxx -p xxxx \
        -r Admin \
        -e donatien.konan.pro@gmail.com \
        -f donatien  \
        -l konan


# Run the scheduler in the background
nohup airflow scheduler &> /dev/null &

# Run the web server in the background
nohup airflow webserver --port 8080 &

