import streamlit as st
from dataclasses import dataclass
from datetime import date
from typing import List
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------------------------------------------------------------
# DB CONFIG ‑ adjust if your credentials differ
# ------------------------------------------------------------------------
DB_PARAMS = dict(
    dbname="airflow",  # ← same Postgres DB the rest of your app uses
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432",
)

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS saving_goals (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    target_date DATE NOT NULL,
    target_amount NUMERIC NOT NULL
);
"""
SEED_SQL = """
INSERT INTO saving_goals (name, target_date, target_amount)
VALUES ('Emergency Fund', '2100-01-01', 15000)
ON CONFLICT (name) DO NOTHING;
"""

# ------------------------------------------------------------------------
# Data model
# ------------------------------------------------------------------------
@dataclass
class Goal:
    id: int | None
    name: str
    target_date: date
    target_amount: float
    allocation: float = 0.0  # calculated, not stored

# ------------------------------------------------------------------------
# Helpers – connection & bootstrap
# ------------------------------------------------------------------------
@st.cache_resource
def get_conn():
    """One connection per session (cached as a resource)."""
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(TABLE_SQL)
        cur.execute(SEED_SQL)
    return conn

# ------------------------------------------------------------------------
# Cached data fetch – returns ONLY plain dicts (pickle‑safe)
# ------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def fetch_goals() -> list[dict]:
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name, target_date, target_amount
            FROM saving_goals
            ORDER BY CASE WHEN lower(name) = 'emergency fund' THEN 0 ELSE 1 END,
                     target_date
            """
        )
        rows = cur.fetchall()
    return rows  # pickle‑serialisable list[dict]


def dicts_to_goals(rows: list[dict]) -> List[Goal]:
    clean_rows: List[Goal] = []
    for row in rows:
        tgt_amt = row["target_amount"]
        # Ensure we work with floats, not Decimal, to avoid math type errors
        if isinstance(tgt_amt, Decimal):
            tgt_amt = float(tgt_amt)
        clean_rows.append(
            Goal(
                id=row["id"],
                name=row["name"],
                target_date=row["target_date"],
                target_amount=tgt_amt,
            )
        )
    return clean_rows

# ------------------------------------------------------------------------
# Waterfall allocation (Option A)
# ------------------------------------------------------------------------
def allocate_cash(total_balance: float, goals: List[Goal]) -> List[Goal]:
    catch_all = next((g for g in goals if g.name.lower() == "emergency fund"), None)
    others = [g for g in goals if g is not catch_all]
    others.sort(key=lambda g: g.target_date)

    remaining = float(total_balance)
    for g in others:
        need = max(g.target_amount - g.allocation, 0)
        g.allocation = min(need, remaining)
        remaining -= g.allocation

    if catch_all:
        catch_all.allocation = remaining
        ordered = [catch_all] + others
    else:
        ordered = others
    return ordered

# ------------------------------------------------------------------------
# Write helpers (invalidate cache afterwards)
# ------------------------------------------------------------------------

def add_goal(name: str, target_date: date, target_amount: float):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO saving_goals (name, target_date, target_amount)
                 VALUES (%s, %s, %s)
                 ON CONFLICT (name) DO NOTHING""",
            (name, target_date, target_amount),
        )
    fetch_goals.clear()  # refresh cache


def delete_goal(goal_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM saving_goals WHERE id = %s", (goal_id,))
    fetch_goals.clear()


def update_goal(goal_id: int, target_date: date, target_amount: float):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE saving_goals
                   SET target_date = %s, target_amount = %s
                 WHERE id = %s""",
            (target_date, target_amount, goal_id),
        )
    fetch_goals.clear()

# ------------------------------------------------------------------------
# Streamlit UI
# ------------------------------------------------------------------------

def main():
    st.set_page_config(page_title="Savings Goals", page_icon="💰")
    st.title("💰 Savings Goal Manager")

    # 1️⃣ Balance input
    total_balance = st.number_input(
        "Savings account balance", value=10_000.0, step=100.0, format="%f"
    )

    # 2️⃣ Add goal form
    with st.expander("➕ Add a new goal"):
        name = st.text_input("Goal name")
        col_d, col_a = st.columns(2)
        with col_d:
            tgt_date = st.date_input("Target date", value=date(2025, 12, 1))
        with col_a:
            tgt_amt = st.number_input("Target amount ($)", min_value=0.0, step=50.0)
        if st.button("Add goal") and name.strip():
            add_goal(name.strip(), tgt_date, tgt_amt)
            st.experimental_rerun()

    # 3️⃣ Load goals + allocate
    goals = dicts_to_goals(fetch_goals())
    goals = allocate_cash(total_balance, goals)

    # 4️⃣ Display cards
    for g in goals:
        st.subheader(f"{g.name} — ${g.allocation:,.0f} / ${g.target_amount:,.0f}")
        progress_val = 0.0
        if g.target_amount > 0:
            progress_val = min(g.allocation / float(g.target_amount), 1.0)
        st.progress(progress_val)

        if g.name.lower() != "emergency fund":
            col_del, col_edit = st.columns(2)
            if col_del.button("Delete", key=f"del_{g.id}"):
                delete_goal(g.id)
                st.experimental_rerun()
            if col_edit.button("✏️ Edit", key=f"edit_{g.id}"):
                with st.modal(f"Edit {g.name}"):
                    new_amt = st.number_input(
                        "Target amount ($)", value=float(g.target_amount), step=50.0
                    )
                    new_dt = st.date_input("Target date", value=g.target_date)
                    if st.button("Save changes"):
                        update_goal(g.id, new_dt, new_amt)
                        st.experimental_rerun()


if __name__ == "__main__":
    main()
