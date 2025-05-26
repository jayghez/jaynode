import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
from datetime import datetime

from finance_utils import (
    normalize,
    save_to_db,
    load_recent,
    DB_PARAMS          # ← already declared in finance_utils.py
)

st.set_page_config(layout="wide")
st.title("💸 Personal Finance Uploader & Dashboard")

# ────────────────────────────────────────────────
# 1.  CSV UPLOAD  +  SAVE
# ────────────────────────────────────────────────
uploaded_file = st.file_uploader("Upload a CSV statement", type="csv")
source = st.selectbox("Bank source",[ "Pre-merged Union"])

if uploaded_file:
    df_raw = pd.read_csv(uploaded_file)
    df_norm = normalize(df_raw, source)
    st.subheader("Preview")
    st.dataframe(df_norm.head())
    if st.button("Save to Postgres"):
        save_to_db(df_norm)
        st.success("✅ Saved!")

# ────────────────────────────────────────────────
# 2.  TABLE-LEVEL KPIs + DELETE BUTTON
# ────────────────────────────────────────────────
@st.cache_data
def table_stats():
    with psycopg2.connect(**DB_PARAMS) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)        AS row_cnt,
                   MIN(transaction_date) AS min_dt,
                   MAX(transaction_date) AS max_dt
            FROM airflow_data.transactions
        """)
        return cur.fetchone()          # (row_cnt, min_dt, max_dt)

row_cnt, min_dt, max_dt = table_stats()

col1, col2, col3 = st.columns(3)
col1.metric("🔢 Rows", f"{row_cnt:,}")
col2.metric("📅 First Date", min_dt.strftime("%Y-%m-%d") if min_dt else "—")
col3.metric("📅 Last Date",  max_dt.strftime("%Y-%m-%d")  if max_dt else "—")

delete_clicked = st.button("⚠️ Delete ALL transactions (TRUNCATE)")
if delete_clicked:
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE airflow_data.transactions")
        conn.commit()
    st.cache_data.clear()   # wipe KPI + chart caches
    st.warning("Table truncated. Reload the page to see an empty state.")

# ────────────────────────────────────────────────
# 3.  LATEST TRANSACTIONS PREVIEW
# ────────────────────────────────────────────────
st.subheader("📊 Latest Transactions")
st.dataframe(load_recent())

# ────────────────────────────────────────────────
# 4.  LINE CHART — NET AMOUNT BY SOURCE OVER TIME
# ────────────────────────────────────────────────
@st.cache_data
def load_chart_data():
    with psycopg2.connect(**DB_PARAMS) as conn:
        df = pd.read_sql("""
            SELECT transaction_date, source, amount_changed
            FROM airflow_data.transactions
        """, conn)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    return (
        df.groupby(['transaction_date', 'source'])['amount_changed']
          .sum()
          .reset_index()
    )

chart_df = load_chart_data()
st.subheader("📈 Net Amount Over Time by Source")
fig, ax = plt.subplots()
sns.lineplot(
    data=chart_df,
    x="transaction_date",
    y="amount_changed",
    hue="source",
    marker="o",
    ax=ax
)
ax.set_xlabel("Date")
ax.set_ylabel("Net Amount ($)")
ax.yaxis.set_major_formatter("${x:,.0f}")
ax.set_title("Daily Net Inflow / Outflow")
st.pyplot(fig, use_container_width=True)
