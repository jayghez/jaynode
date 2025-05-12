import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

st.set_page_config(layout="wide")
st.title("📊 Personal Finance Insights")

# DB connection settings
DB_PARAMS = dict(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432"
)

@st.cache_data

def load_data():
    conn = psycopg2.connect(**DB_PARAMS)
    df = pd.read_sql("SELECT * FROM airflow_data.transactions", conn)
    conn.close()
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['month'] = df['transaction_date'].dt.to_period('M')
    df['week'] = df['transaction_date'].dt.to_period('W')
    return df

# Load data
df = load_data()

# Filters
st.sidebar.header("Filters")

min_date = df['transaction_date'].min()
max_date = df['transaction_date'].max()

start_date, end_date = st.sidebar.date_input("Date range", [min_date, max_date])
sources = st.sidebar.multiselect("Source", df['source'].unique(), default=list(df['source'].unique()))
categories = st.sidebar.multiselect("Category", df['category'].dropna().unique(), default=list(df['category'].dropna().unique()))

filtered = df[
    (df['transaction_date'] >= pd.to_datetime(start_date)) &
    (df['transaction_date'] <= pd.to_datetime(end_date)) &
    (df['source'].isin(sources)) &
    (df['category'].isin(categories))
]

# Summary KPIs
col1, col2 = st.columns(2)

with col1:
    st.metric("💸 Total Spending", f"${-filtered[filtered['transaction_type']=='Spending']['amount_changed'].sum():,.2f}")
with col2:
    st.metric("💰 Total Income", f"${filtered[filtered['transaction_type']=='Income']['amount_changed'].sum():,.2f}")

# Weekly trend
st.subheader("🗓️ Weekly Spend Trend")
weekly = (
    filtered[(filtered['transaction_type']=='Spending') & (filtered['type']!='Payment')]
    .groupby('week')['amount_changed']
    .sum()
    .reset_index()
)
sns.set_theme()
fig1, ax1 = plt.subplots()
sns.lineplot(data=weekly, x='week', y='amount_changed', marker="o", ax=ax1)
ax1.set_ylabel("Weekly Spend ($)")
st.pyplot(fig1)

# Monthly category-source totals
st.subheader("🏷️ Spend by Category and Source")
category_source_totals = (
    filtered.groupby(['category', 'source'])['amount_changed']
    .sum()
    .reset_index()
    .sort_values(by='amount_changed', ascending=False)
)
st.dataframe(category_source_totals, use_container_width=True)

# Heatmap of spend by month and source
st.subheader("📆 Heatmap: Monthly Spend by Source")
heatmap_data = (
    filtered[filtered['transaction_type']=='Spending']
    .groupby(['month', 'source'])['amount_changed']
    .sum()
    .unstack()
    .fillna(0)
)
fig2, ax2 = plt.subplots(figsize=(10, 4))
sns.heatmap(heatmap_data.T, cmap="Reds", linewidths=0.5, annot=True, fmt=".0f")
ax2.set_xlabel("Month")
ax2.set_ylabel("Source")
st.pyplot(fig2)

# Transaction table
st.subheader("🧾 Filtered Transactions")
st.dataframe(filtered.sort_values(by='transaction_date', ascending=False), use_container_width=True)