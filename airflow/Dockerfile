FROM python:3.12.8-slim

# ------------------------
# Environment
# ------------------------
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.10.3
ENV PYTHON_VERSION=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# ------------------------
# Install system dependencies
# ------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libpq-dev \
        poppler-utils \
        tesseract-ocr \
        dos2unix \
    && rm -rf /var/lib/apt/lists/*

# ------------------------
# Create airflow user
# ------------------------
RUN groupadd -r -g 50000 airflow && \
    useradd -r -u 50000 -g airflow -d ${AIRFLOW_HOME} -s /bin/bash airflow

# ------------------------
# Create directories
# ------------------------
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins && \
    chown -R airflow:airflow ${AIRFLOW_HOME} && \
    chmod -R 755 ${AIRFLOW_HOME}

# ------------------------
# Install Airflow + psycopg2
# ------------------------
RUN pip install --no-cache-dir \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}" \
    psycopg2-binary

# ------------------------
# Copy requirements & install
# ------------------------
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# ------------------------
# Copy entrypoint
# ------------------------
COPY entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh && chmod +x /entrypoint.sh

# ------------------------
# Switch to airflow user
# ------------------------
USER airflow
WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080
CMD ["/entrypoint.sh"]
