FROM apache/airflow:2.10.0

USER root
RUN apt-get update && apt-get install -y postgresql-client --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .