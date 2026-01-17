FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && apt-get install -y gcc g++ curl && apt-get clean
USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt \
    -r /requirements.txt
