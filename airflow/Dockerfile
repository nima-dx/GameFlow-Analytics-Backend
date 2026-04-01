FROM python:3.12.8-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1

ENV AIRFLOW_HOME=/app/airflow

# Install curl
RUN apt-get update && apt-get install -y curl

WORKDIR $AIRFLOW_HOME

COPY airflow/scripts scripts
RUN chmod +x scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main --no-root
