FROM python:3.12.8-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV DBT_HOME=/app/dbt_gameflow

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY airflow/scripts airflow/scripts
RUN chmod +x airflow/scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./
COPY ingestion ./ingestion
COPY dbt_gameflow ./dbt_gameflow

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main --no-root

RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar -xzf spark-3.5.0-bin-hadoop3.tgz \
    && mv spark-3.5.0-bin-hadoop3 /opt/spark \
    && rm spark-3.5.0-bin-hadoop3.tgz

WORKDIR ${AIRFLOW_HOME}
