FROM apache/airflow:2.7.2

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-http \
    redis \
    apache-airflow[celery]

# Ensure correct permissions
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/logs \
    && chown -R airflow:root /opt/airflow