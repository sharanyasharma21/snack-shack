FROM apache/airflow:3.0.0-python3.11

USER root
RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root src/ /opt/airflow/src/
