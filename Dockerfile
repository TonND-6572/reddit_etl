FROM apache/airflow:2.10.2-python3.12

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update && apt-get install -y gcc python3-dev

USER airflow

USER root
RUN mkdir -p /home/airflow/.aws/
COPY credentials /home/airflow/.aws/

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt