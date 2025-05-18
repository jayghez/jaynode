import psycopg2

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