# Base Image
FROM python:3.11-slim
LABEL maintainer="DonatienKonan"

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=2.9.1
ARG AIRFLOW_HOME=/opt/airflow

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install dependencies and tools you might use
RUN apt-get update -yqq && \
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


# copy requirements file
COPY ./requirements.txt requirements.txt
RUN pip install --upgrade pip && \
    pip install apache-airflow[postgres]==${AIRFLOW_VERSION} && \
    pip install -r requirements.txt

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./entrypoint.sh ./entrypoint.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./entrypoint.sh

# Set the owner of the files in AIRFLOW_HOME to the user airflow
#RUN chown -R airflow:airflow ${AIRFLOW_HOME}

# Set the username to use
#USER airflow

# Set workdir (it's like a cd inside the container)
WORKDIR ${AIRFLOW_HOME}
ADD ./data /data

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

VOLUME [ "/data" ]

# Execute the entrypoint.sh
ENTRYPOINT [ "/entrypoint.sh" ]
