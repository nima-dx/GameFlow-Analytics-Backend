FROM python:3.12.8-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1

ENV AIRFLOW_HOME=/app/airflow


# Install curl
RUN apt-get update && apt-get install -y curl

WORKDIR /app

COPY airflow/scripts airflow/scripts
RUN chmod +x airflow/scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./
COPY ingestion ./ingestion

WORKDIR $AIRFLOW_HOME

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main --no-root\
