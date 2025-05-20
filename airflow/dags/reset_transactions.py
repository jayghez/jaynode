from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def reset_transactions_table():
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS airflow_data.transactions;")
    cursor.execute("""
    CREATE TABLE airflow_data.transactions (
        transaction_id TEXT PRIMARY KEY,
        transaction_date DATE,
        description TEXT,
        category TEXT,
        type TEXT,
        amount NUMERIC,
        source TEXT,
        transaction_type TEXT,
        amount_changed NUMERIC
    );
    """)
    conn.commit()
    conn.close()
    print("✅ Table reset complete.")

with DAG(
    dag_id='reset_transactions',
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False
) as dag:

    reset_table = PythonOperator(
        task_id='reset_transactions_table',
        python_callable=reset_transactions_table
    )