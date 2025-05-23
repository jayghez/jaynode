import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from datetime import datetime
from finance_utils import DB_PARAMS

st.set_page_config(layout="wide")
st.title("📊 Personal Finance Insights")

# ────────────────────────────────────────────────
# 1.  LOAD DATA (cached)
# ────────────────────────────────────────────────
@st.cache_data
def load_data():
    with psycopg2.connect(**DB_PARAMS) as conn:
        df = pd.read_sql("SELECT * FROM airflow_data.transactions", conn)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['month'] = df['transaction_date'].dt.to_period('M').dt.start_time
    df['week']  = df['transaction_date'].dt.to_period('W').dt.start_time
    return df

df = load_data()

# ────────────────────────────────────────────────
# 2.  SIDEBAR FILTERS
# ────────────────────────────────────────────────
st.sidebar.header("Filters")
min_dt, max_dt = df['transaction_date'].min(), df['transaction_date'].max()
start_dt, end_dt = st.sidebar.date_input("Date range", [min_dt, max_dt])
sources    = st.sidebar.multiselect("Source",    df['source'].unique(), default=list(df['source'].unique()))
categories = st.sidebar.multiselect("Category",  df['category'].dropna().unique(), default=list(df['category'].dropna().unique()))
types      = st.sidebar.multiselect("Type",      df['type'].dropna().unique(),       default=list(df['type'].dropna().unique()))

filtered = df[
    (df['transaction_date'] >= pd.to_datetime(start_dt)) &
    (df['transaction_date'] <= pd.to_datetime(end_dt)) &
    (df['source'].isin(sources)) &
    (df['category'].isin(categories)) &
    (df['type'].isin(types))
]

# ────────────────────────────────────────────────
# 3.  TOP-LEVEL KPIs
# ────────────────────────────────────────────────
col1, col2 = st.columns(2)
col1.metric("💸 Total Spending", f"${-filtered[filtered['transaction_type']=='Spending']['amount_changed'].sum():,.2f}")
col2.metric("💰 Total Income",   f"${ filtered[filtered['transaction_type']=='Income'  ]['amount_changed'].sum():,.2f}")

currency_fmt = FuncFormatter(lambda x, _: f"${x:,.0f}")

# ────────────────────────────────────────────────
# 4.  WEEKLY TREND (LINE)
# ────────────────────────────────────────────────
st.subheader("🗓️ Weekly Spend Trend")
weekly = (
    filtered.query("transaction_type == 'Spending' and type != 'Payment'")
            .groupby('week')['amount_changed']
            .sum()
            .reset_index()
)
sns.set_theme()
fig1, ax1 = plt.subplots()
sns.lineplot(data=weekly, x='week', y='amount_changed', marker="o", ax=ax1)
ax1.set_ylabel("Weekly Spend ($)")
ax1.yaxis.set_major_formatter(currency_fmt)
ax1.invert_yaxis()
st.pyplot(fig1)

# ────────────────────────────────────────────────
# 5.  CATEGORY × SOURCE  TABLE
# ────────────────────────────────────────────────
st.subheader("🏷️ Spend by Category and Source")
cat_src = (
    filtered.groupby(['category', 'source'])['amount_changed']
            .sum()
            .reset_index()
            .sort_values(by='amount_changed', ascending=False)
)
st.dataframe(
    cat_src.style.format({'amount_changed': '${:,.2f}'}),
    use_container_width=True
)

# ────────────────────────────────────────────────
# 6.  HEATMAP  (MONTH × SOURCE)
# ────────────────────────────────────────────────
st.subheader("📆 Heatmap: Monthly Spend by Source")
heat = (
    filtered.query("transaction_type == 'Spending'")
            .groupby(['month', 'source'])['amount_changed']
            .sum()
            .unstack()
            .fillna(0)
)
fig2, ax2 = plt.subplots(figsize=(10, 4))
sns.heatmap(
    heat.T,
    cmap="Reds",
    linewidths=0.5,
    annot=True,
    fmt='$,.0f'
)
ax2.set_xlabel("Month")
ax2.set_ylabel("Source")
ax2.set_xticklabels([d.strftime("%b %Y") for d in heat.index], rotation=45, ha='right')
st.pyplot(fig2)

# ────────────────────────────────────────────────
# 7.  FILTERED TRANSACTIONS TABLE
# ────────────────────────────────────────────────
st.subheader("🧾 Filtered Transactions")

visible_cols = (
    filtered
      .sort_values(by='transaction_date', ascending=False)
      .loc[:, ['transaction_date', 'source', 'category', 'transaction_type', 'amount_changed']]
      .rename(columns={
          'transaction_date': 'Transaction Date',
          'source':           'Source',
          'category':         'Category',
          'transaction_type': 'Transaction Type',
          'amount_changed':   'Amount Charged'
      })
)

st.dataframe(
    visible_cols.style.format({'Amount Charged': '${:,.2f}'}),
    use_container_width=True
)
