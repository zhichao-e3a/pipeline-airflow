FROM apache/airflow:3.1.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_VERSION=3.12
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
ENV MODE=local
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
