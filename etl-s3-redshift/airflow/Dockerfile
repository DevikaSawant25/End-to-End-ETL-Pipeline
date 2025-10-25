FROM apache/airflow:2.9.2
USER root
# Add any OS deps if needed
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
