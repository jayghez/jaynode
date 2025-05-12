import pandas as pd
import psycopg2
import hashlib
from datetime import datetime

DB_PARAMS = dict(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432"
)
def normalize(df, source):
    mappings = {
        "usaa": {"Date": "Transaction Date", "Description": "Description", "Category": "Type", "Amount": "Amount"},
        "chase": {},  # Already standardized
        "apple": {"Transaction Date": "Transaction Date", "Clearing Date": "Post Date", "Description": "Description", "Merchant": "Merchant", "Amount (USD)": "Amount"},
        "frost": {},  # Already standardized
        "american_express": {"Transaction Date": "Transaction Date", "Clearing Date": "Post Date", "Description": "Description", "Merchant": "Merchant", "Amount (USD)": "Amount"},
    }

    if source.lower() == "pre-merged union":
        pass
    else:
        if source.lower() in mappings:
            df = df.rename(columns=mappings[source.lower()])
        df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
        df['source'] = source

        if 'Merchant' in df.columns:
            df['Description'] = df.apply(
                lambda row: f"{row['Description']} - {row['Merchant']}" if pd.notna(row['Merchant']) else row['Description'], axis=1
            )

    df['Amount_Changed'] = (
        df['Amount']
        .fillna('0')
        .astype(str)
        .str.replace('[\$,]', '', regex=True)
        .str.strip()
        .replace('', '0')
        .astype(float)
    )

    df['Transaction Type'] = df['Amount_Changed'].apply(lambda x: 'Income' if x > 0 else 'Spending')
    df['Amount'] = df['Amount_Changed']

    def generate_transaction_id(row):
        uid = f"{row['Transaction Date']}_{row['Amount_Changed']}_{row['Description']}_{row['source']}"
        return hashlib.sha256(uid.encode()).hexdigest()

    df['transaction_id'] = df.apply(generate_transaction_id, axis=1)

    return df[[
        'transaction_id', 'Transaction Date', 'Description', 'Category', 'Type',
        'Amount', 'source', 'Transaction Type', 'Amount_Changed'
    ]]

def save_to_db(df):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS airflow_data;
    CREATE TABLE IF NOT EXISTS airflow_data.transactions (
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

    for _, row in df.iterrows():
        cursor.execute("""
        INSERT INTO airflow_data.transactions (
            transaction_id, transaction_date, description, category, type,
            amount, source, transaction_type, amount_changed
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
        """, (
            row['transaction_id'], row['Transaction Date'], row['Description'], row['Category'], row['Type'],
            row['Amount'], row['source'], row['Transaction Type'], row['Amount_Changed']
        ))

    conn.commit()
    conn.close()

def load_recent(n=20):
    conn = psycopg2.connect(**DB_PARAMS)
    df = pd.read_sql(f"SELECT * FROM airflow_data.transactions ORDER BY transaction_date DESC LIMIT {n};", conn)
    conn.close()
    return df