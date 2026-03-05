FROM apache/airflow:2.8.4

USER root
RUN apt-get update && apt-get install -y gcc && apt-get clean
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt