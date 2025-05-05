FROM apache/airflow:2.9.1-python3.11

# No custom Python deps for now
USER root
RUN pip install --no-cache-dir psycopg2-binary
USER airflow