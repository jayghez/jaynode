import pandas as pd
import psycopg2
import hashlib
from datetime import datetime

# ────────────────────────────────────────────────
# Database connection parameters (Docker‑compose defaults)
# ────────────────────────────────────────────────
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432",
}

# ────────────────────────────────────────────────
# Helper functions for one‑off column cleaning
# ────────────────────────────────────────────────

def clean_category(cat: str) -> str:
    """Collapse messy bank categories into a smaller, opinionated set."""
    if pd.isna(cat):
        return "Uncategorized"
    cat = cat.lower().strip()
    if any(k in cat for k in ("food", "restaurant", "drink")):
        return "Food & Drink"
    if any(k in cat for k in ("gas", "fuel")):
        return "Gas"
    if "grocery" in cat:
        return "Groceries"
    if any(k in cat for k in ("travel", "airline", "hotel")):
        return "Travel"
    if any(k in cat for k in ("entertainment", "movies", "theater")):
        return "Entertainment"
    if any(k in cat for k in ("utility", "bill")):
        return "Utilities"
    if any(k in cat for k in ("health", "medical")):
        return "Health & Wellness"
    if any(k in cat for k in ("shop", "retail", "clothing")):
        return "Shopping"
    if any(k in cat for k in ("fees", "adjustment", "charge")):
        return "Fees & Adjustments"
    if any(k in cat for k in ("donation", "gift")):
        return "Gifts & Donations"
    if any(k in cat for k in ("personal", "home", "auto")):
        return "Personal & Home"
    if "misc" in cat:
        return "Misc"
    return cat.title()

def clean_type(tp: str) -> str:
    """Standardise the bank‑specific *Type* column."""
    if pd.isna(tp):
        return "Other"
    tp = tp.lower().strip()
    if any(k in tp for k in ("deposit", "income", "return")):
        return "Income"
    if any(k in tp for k in ("payment", "withdrawal", "purchase", "debit")):
        return "Spending"
    if "transfer" in tp:
        return "Transfer"
    if "interest" in tp:
        return "Interest"
    return tp.title()

def clean_amount(row: pd.Series) -> float:
    """Convert an *Amount* string into a signed float from *your* perspective."""
    amt_str = str(row["Amount"]).replace("$", "").replace(",", "").strip()
    try:
        value = float(amt_str)
        raw_type = str(row.get("Type", "")).lower()
        if raw_type in ("income", "deposit", "return", "credit"):
            return abs(value)
        if raw_type in ("payment", "withdrawal", "debit", "purchase"):
            return -abs(value)
        # Fallback: use sign as‑is
        return value
    except ValueError:
        return 0.0

# ────────────────────────────────────────────────
# Normalisation pipeline for uploaded CSVs
# ────────────────────────────────────────────────

def normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Takes a raw bank CSV → returns a fully‑cleaned, uniform DataFrame."""

    # Column‑rename map per source (extend as needed)
    mappings = {
        "usaa": {
            "Date": "Transaction Date",
            "Description": "Description",
            "Category": "Category",
            "Amount": "Amount",
        },
        "chase": {},  # already in preferred format after Plaid export
        "apple": {
            "Transaction Date": "Transaction Date",
            "Clearing Date": "Post Date",
            "Description": "Description",
            "Merchant": "Merchant",
            "Amount (USD)": "Amount",
        },
        "frost": {},
        "american_express": {
            "Transaction Date": "Transaction Date",
            "Clearing Date": "Post Date",
            "Description": "Description",
            "Merchant": "Merchant",
            "Amount (USD)": "Amount",
        },
    }

    src_key = source.lower()
    if src_key != "pre-merged union" and src_key in mappings:
        df = df.rename(columns=mappings[src_key])
        df["Transaction Date"] = pd.to_datetime(df["Transaction Date"])
        df["source"] = source.title()

        # If merchant present, append to description for richer search text
        if "Merchant" in df.columns:
            df["Description"] = df.apply(
                lambda r: f"{r['Description']} - {r['Merchant']}" if pd.notna(r["Merchant"]) else r["Description"],
                axis=1,
            )

    # Core cleans
    df["Category"] = df["Category"].apply(clean_category)
    df["Type"] = df["Type"].apply(clean_type)
    df["Amount_Changed"] = df.apply(clean_amount, axis=1)

    # Base classifier: sign‑based Income vs Spending
    df["Transaction Type"] = df["Amount_Changed"].apply(lambda x: "Income" if x > 0 else "Spending")

    # ─── Override: card‑issuer mobile CC payments → always Payment ───
    mask_mobile_pay = df["Description"].str.contains("Payment Thank You-Mobile -", case=False, na=False)
    df.loc[mask_mobile_pay, "Transaction Type"] = "Payment"
    df.loc[mask_mobile_pay, "Type"] = "Payment"

    # Convenience duplicate of Amount_Changed (legacy dashboards expect both)
    df["Amount"] = df["Amount_Changed"]

    # Generate a deterministic transaction_id hash
    def _make_id(r):
        uid = f"{r['Transaction Date']}_{r['Amount_Changed']}_{r['Description']}_{r['source']}"
        return hashlib.sha256(uid.encode()).hexdigest()

    df["transaction_id"] = df.apply(_make_id, axis=1)

    return df[
        [
            "transaction_id",
            "Transaction Date",
            "Description",
            "Category",
            "Type",
            "Amount",
            "source",
            "Transaction Type",
            "Amount_Changed",
        ]
    ]

# ────────────────────────────────────────────────
# Postgres helpers
# ────────────────────────────────────────────────

def save_to_db(df: pd.DataFrame) -> None:
    """Insert a normalised DataFrame into airflow_data.transactions (idempotent)."""
    with psycopg2.connect(**DB_PARAMS) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS airflow_data;
            CREATE TABLE IF NOT EXISTS airflow_data.transactions (
                transaction_id     TEXT PRIMARY KEY,
                transaction_date   DATE,
                description        TEXT,
                category           TEXT,
                type               TEXT,
                amount             NUMERIC,
                source             TEXT,
                transaction_type   TEXT,
                amount_changed     NUMERIC
            );
            """
        )

        for _, r in df.iterrows():
            cur.execute(
                """
                INSERT INTO airflow_data.transactions (
                    transaction_id, transaction_date, description, category, type,
                    amount, source, transaction_type, amount_changed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING;
                """,
                (
                    r["transaction_id"],
                    r["Transaction Date"],
                    r["Description"],
                    r["Category"],
                    r["Type"],
                    r["Amount"],
                    r["source"],
                    r["Transaction Type"],
                    r["Amount_Changed"],
                ),
            )
        conn.commit()


def load_recent(n: int = 20) -> pd.DataFrame:
    """Return the *n* most recent transactions (for dashboard preview)."""
    with psycopg2.connect(**DB_PARAMS) as conn:
        q = "SELECT * FROM airflow_data.transactions ORDER BY transaction_date DESC LIMIT %s;"
        return pd.read_sql(q, conn, params=(n,))

# ────────────────────────────────────────────────
# Subscription / recurring‑charge detector
# ────────────────────────────────────────────────

def find_monthly_subscriptions(df: pd.DataFrame, min_months: int = 3) -> pd.DataFrame:
    """Identify outflow transactions that repeat every month for the same amount."""
    spend = df[df["amount_changed"] < 0].copy()
    spend["month"] = pd.to_datetime(spend["transaction_date"]).dt.to_period("M")

    grouped = (
        spend.groupby(["description", "amount_changed"])
             .agg(
                 months=("month", "nunique"),
                 first_month=("month", "min"),
                 last_month=("month", "max"),
             )
             .reset_index()
    )

    subs = grouped[grouped["months"] >= min_months].copy()
    subs.rename(
        columns={
            "description": "Description",
            "amount_changed": "Amount",
            "months": "Months",
            "first_month": "First Month",
            "last_month": "Last Month",
        },
        inplace=True,
    )

    # Positive dollars for readability
    subs["Amount"] = subs["Amount"].abs()
    subs.sort_values(by="Amount", ascending=False, inplace=True)
    return subs
