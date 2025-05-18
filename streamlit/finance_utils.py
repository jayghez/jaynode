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

def clean_category(cat):
    if pd.isna(cat):
        return 'Uncategorized'
    cat = cat.lower().strip()
    if 'food' in cat or 'restaurant' in cat or 'drink' in cat:
        return 'Food & Drink'
    elif 'gas' in cat or 'fuel' in cat:
        return 'Gas'
    elif 'grocery' in cat:
        return 'Groceries'
    elif 'travel' in cat or 'airline' in cat or 'hotel' in cat:
        return 'Travel'
    elif 'entertainment' in cat or 'movies' in cat or 'theater' in cat:
        return 'Entertainment'
    elif 'utility' in cat or 'bill' in cat:
        return 'Utilities'
    elif 'health' in cat or 'medical' in cat:
        return 'Health & Wellness'
    elif 'shop' in cat or 'retail' in cat or 'clothing' in cat:
        return 'Shopping'
    elif 'fees' in cat or 'adjustment' in cat or 'charge' in cat:
        return 'Fees & Adjustments'
    elif 'donation' in cat or 'gift' in cat:
        return 'Gifts & Donations'
    elif 'personal' in cat or 'home' in cat or 'auto' in cat:
        return 'Personal & Home'
    elif 'misc' in cat:
        return 'Misc'
    return cat.title()

def clean_type(tp):
    if pd.isna(tp):
        return 'Other'
    tp = tp.lower().strip()
    if 'deposit' in tp or 'income' in tp or 'return' in tp:
        return 'Income'
    elif 'payment' in tp or 'withdrawal' in tp or 'purchase' in tp or 'debit' in tp:
        return 'Spending'
    elif 'transfer' in tp:
        return 'Transfer'
    elif 'interest' in tp:
        return 'Interest'
    return tp.title()

def clean_amount(row):
    amt = str(row['Amount']).replace('$', '').replace(',', '').strip()
    try:
        value = float(amt)
        if row['Type'].lower() in ['income', 'deposit', 'return', 'credit']:
            return abs(value)
        elif row['Type'].lower() in ['payment', 'withdrawal', 'debit', 'purchase']:
            return -abs(value)
        else:
            return value
    except:
        return 0.0

def normalize(df, source):
    mappings = {
        "usaa": {"Date": "Transaction Date", "Description": "Description", "Category": "Type", "Amount": "Amount"},
        "chase": {},
        "apple": {"Transaction Date": "Transaction Date", "Clearing Date": "Post Date", "Description": "Description", "Merchant": "Merchant", "Amount (USD)": "Amount"},
        "frost": {},
        "american_express": {"Transaction Date": "Transaction Date", "Clearing Date": "Post Date", "Description": "Description", "Merchant": "Merchant", "Amount (USD)": "Amount"},
    }

    if source.lower() != "pre-merged union" and source.lower() in mappings:
        df = df.rename(columns=mappings[source.lower()])
        df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
        df['source'] = source

        if 'Merchant' in df.columns:
            df['Description'] = df.apply(
                lambda row: f"{row['Description']} - {row['Merchant']}" if pd.notna(row['Merchant']) else row['Description'], axis=1
            )

    df['Category'] = df['Category'].apply(clean_category)
    df['Type'] = df['Type'].apply(clean_type)
    df['Amount_Changed'] = df.apply(clean_amount, axis=1)
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
