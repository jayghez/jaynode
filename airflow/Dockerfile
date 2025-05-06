FROM apache/airflow:2.9.1-python3.11

# Copy your DAGs into the correct Airflow DAG folder
COPY dags /opt/airflow/dags

# Optional: install any additional python packages
RUN pip install requests pandas