# pages/Insights.py   — drop-in replacement
import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from finance_utils import DB_PARAMS

st.set_page_config(layout="wide")
st.title("📊 Personal Finance Insights")

# ────────────────────────────────────────────────
# 1 ▸ LOAD + CLEAN DATA  (cached)
# ────────────────────────────────────────────────
@st.cache_data
def load_data():
    with psycopg2.connect(**DB_PARAMS) as conn:
        df = pd.read_sql("SELECT * FROM airflow_data.transactions", conn)

    # ——— standard parsing ———
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["month"] = df["transaction_date"].dt.to_period("M").dt.start_time
    df["week"] = df["transaction_date"].dt.to_period("W").dt.start_time

    # ——— remove duplicates ———
    subset_cols = ["transaction_id"] if "transaction_id" in df.columns else None
    df = df.drop_duplicates(subset=subset_cols)

    return df

df = load_data()

# ────────────────────────────────────────────────
# 2 ▸ SIDEBAR FILTERS
# ────────────────────────────────────────────────
st.sidebar.header("Filters")

min_dt, max_dt = df["transaction_date"].min(), df["transaction_date"].max()
date_start, date_end = st.sidebar.date_input("Date range", [min_dt, max_dt])

sources    = st.sidebar.multiselect("Source",    df["source"].unique(),   default=list(df["source"].unique()))
categories = st.sidebar.multiselect("Category",  df["category"].dropna().unique(), default=list(df["category"].dropna().unique()))
types      = st.sidebar.multiselect("Type",      df["type"].dropna().unique(),      default=list(df["type"].dropna().unique()))

filtered = df[
    (df["transaction_date"] >= pd.to_datetime(date_start)) &
    (df["transaction_date"] <= pd.to_datetime(date_end))   &
    (df["source"].isin(sources)) &
    (df["category"].isin(categories)) &
    (df["type"].isin(types))
]

currency_fmt = FuncFormatter(lambda x, _: f"${x:,.0f}")

# ────────────────────────────────────────────────
# 3 ▸ WEEKLY SPEND  ▸  line per source
# ────────────────────────────────────────────────
st.subheader("📈 Weekly Spend by Source")

weekly = (
    filtered.query("transaction_type == 'Spending' and type != 'Payment'")
            .groupby(["week", "source"])["amount_changed"]
            .sum()
            .reset_index()
)

fig1, ax1 = plt.subplots()
sns.lineplot(
    data=weekly,
    x="week",
    y="amount_changed",
    hue="source",
    marker="o",
    ax=ax1
)
ax1.set_ylabel("Weekly Spend ($)")
ax1.yaxis.set_major_formatter(currency_fmt)
ax1.invert_yaxis()
st.pyplot(fig1, use_container_width=True)

# ────────────────────────────────────────────────
# 4 ▸ MONTH × SOURCE  HEAT-MAP
# ────────────────────────────────────────────────
st.subheader("🔥 Heat-map: Monthly Spend by Source")

heat = (
    filtered.query("transaction_type == 'Spending'")
            .groupby(["month", "source"])["amount_changed"]
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
    fmt=".0f",          # <= no currency sign here → valid format spec
    cbar_kws={"label": "Amount ($)"}
)
ax2.set_xlabel("Month")
ax2.set_ylabel("Source")
ax2.set_xticklabels([d.strftime("%b %Y") for d in heat.index], rotation=45, ha="right")
st.pyplot(fig2, use_container_width=True)

# ────────────────────────────────────────────────
# 5 ▸ FILTERED TRANSACTIONS TABLE  (unchanged)
# ────────────────────────────────────────────────
st.subheader("🧾 Filtered Transactions")

visible_cols = (
    filtered.sort_values(by="transaction_date", ascending=False)
            .loc[:, [
                "transaction_date",
                "source",
                "category",
                "description",        # ← new
                "transaction_type",
                "amount_changed"
            ]]
            .rename(columns={
                "transaction_date": "Transaction Date",
                "source":           "Source",
                "category":         "Category",
                "description":      "Description",      # ← new
                "transaction_type": "Transaction Type",
                "amount_changed":   "Amount Charged"
            })
)

st.dataframe(
    visible_cols.style.format({"Amount Charged": "${:,.2f}"}),
    use_container_width=True,
    hide_index=True
)
# ────────────────────────────────────────────────
# ▸ SPEND BY CATEGORY × SOURCE  (pivot + row total)
# ────────────────────────────────────────────────
st.subheader("🏷️ Spend by Category and Source")

cat_src = (
    filtered.query("transaction_type == 'Spending'")
            .groupby(["category", "source"])["amount_changed"]
            .sum()
            .unstack(fill_value=0)        # columns → sources
)

# Add a Total column and bring it to the front
cat_src["Total"] = cat_src.sum(axis=1)
# make Total the first column
cat_src = cat_src[["Total", *cat_src.columns.drop('Total')]]

# Sort categories by Total descending
cat_src = cat_src.sort_values(by="Total", ascending=False)

# Nice $ formatting for every cell
cat_src_fmt = cat_src.applymap(lambda x: f"${x:,.0f}")

st.dataframe(
    cat_src_fmt,
    use_container_width=True,
)