"""
Savings_Goals.py
---------------------------------------------------------------------
Streamlit page for managing savings "envelopes" with a water‑fall rule.
* Emergency Fund is always displayed first and acts as the catch‑all.
* Other goals are filled fully (Option A) in chronological order.
* Data lives in an existing Postgres DB (same one your other pages use).
---------------------------------------------------------------------
Update highlights (May 26 2025)
* Fix Decimal ⇢ float mismatch causing TypeError in progress bar.
* Replace `st.experimental_rerun` with a robust `_rerun()` fallback that
  works on older Streamlit releases.
* Ensure an Emergency Fund row is always present (auto‑insert if missing).
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------------------------------------------------------
# DB CONFIG  — adjust if credentials differ on your Umbrel instance.
# ------------------------------------------------------------------
DB_PARAMS = dict(
    dbname="airflow",  # ← change if your finance Stack uses a different DB
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432",
)

# ----------------------------- Helpers ----------------------------
@dataclass
class Goal:
    id: int | None  # None until inserted
    name: str
    target_date: date
    target_amount: float  # stored in USD (float OK once converted)
    allocation: float = 0.0  # calculated at runtime

# Fallback rerun helper (for Streamlit < 1.10 where experimental_rerun absent)
def _rerun():
    if hasattr(st, "experimental_rerun"):
        st.experimental_rerun()
    else:
        # Flip a dummy key in session_state to trigger an app rerun
        st.session_state["_force_rerun"] = st.session_state.get("_force_rerun", 0) + 1

# ----------------------- Cached connections -----------------------
@st.cache_resource(show_spinner=False)
def get_conn():
    return psycopg2.connect(**DB_PARAMS)

# ----------------------- DB initialisation ------------------------

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS saving_goals (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                target_date DATE NOT NULL,
                target_amount NUMERIC NOT NULL CHECK (target_amount >= 0)
            );
            """
        )
        conn.commit()
    ensure_emergency_fund()

def ensure_emergency_fund():
    """Insert a default Emergency Fund goal if one is missing."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM saving_goals WHERE lower(name) = 'emergency fund' LIMIT 1;")
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO saving_goals (name, target_date, target_amount) VALUES (%s,%s,%s);",
                ("Emergency Fund", date(2100, 1, 1), Decimal("15000")),
            )
            conn.commit()

# ------------------------- CRUD helpers ---------------------------
@st.cache_data(show_spinner=False)
def fetch_goals() -> List[Goal]:
    """Return a list of Goal objects (pickle‑friendly)."""
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM saving_goals ORDER BY id;")
        rows = cur.fetchall()
    # Convert Decimal → float to avoid math errors
    goals = [
        Goal(
            id=row["id"],
            name=row["name"],
            target_date=row["target_date"],
            target_amount=float(row["target_amount"]),
        )
        for row in rows
    ]
    return goals

def add_goal(name: str, target_date: date, amount: float):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO saving_goals (name, target_date, target_amount) VALUES (%s,%s,%s);",
            (name, target_date, Decimal(str(amount))),
        )
        conn.commit()
    st.cache_data.clear()


def update_goal(goal_id: int, target_date: date, amount: float):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE saving_goals SET target_date=%s, target_amount=%s WHERE id=%s;",
            (target_date, Decimal(str(amount)), goal_id),
        )
        conn.commit()
    st.cache_data.clear()


def delete_goal(goal_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM saving_goals WHERE id=%s;", (goal_id,))
        conn.commit()
    st.cache_data.clear()

# --------------------- Allocation algorithm ----------------------

def allocate_cash(total_balance: float, goals: List[Goal]) -> List[Goal]:
    """Waterfall allocation (Option A). Emergency Fund is catch‑all."""
    catch_all = next((g for g in goals if g.name.lower() == "emergency fund"), None)
    others = [g for g in goals if g is not catch_all]

    # Sort by soonest date
    others.sort(key=lambda g: g.target_date)

    remaining = total_balance
    for g in others:
        need = max(g.target_amount - g.allocation, 0)
        g.allocation = min(need, remaining)
        remaining -= g.allocation

    if catch_all:
        catch_all.allocation = remaining
        display_order = [catch_all] + others
    else:
        display_order = others
    return display_order

# ------------------------- Streamlit UI ---------------------------

def main():
    st.set_page_config(page_title="Savings Goals", page_icon="💰", layout="centered")
    st.title("💰 Savings Goal Manager")

    init_db()

    # ------------------ Account balance -----------------------
    total_balance = st.number_input("Savings account balance ($)", value=10000.0, step=50.0)

    # ------------------ Add a new goal ------------------------
    with st.expander("➕ Add a new goal"):
        name = st.text_input("Goal name")
        target_date = st.date_input("Target date", value=date(2025, 12, 1))
        amount = st.number_input("Target amount ($)", min_value=0.0, step=50.0)
        if st.button("Add goal"):
            if not name.strip():
                st.warning("Name cannot be empty.")
            else:
                add_goal(name, target_date, amount)
                _rerun()

    # ------------------ Load, allocate, display ---------------
    goals = fetch_goals()
    goals = allocate_cash(total_balance, goals)

    for g in goals:
        st.subheader(f"{g.name} — ${g.allocation:,.0f} / ${g.target_amount:,.0f}")
        progress = 0.0 if g.target_amount == 0 else min(g.allocation / g.target_amount, 1.0)
        st.progress(progress)

        if g.name.lower() != "emergency fund":
            col1, col2 = st.columns(2)
            if col1.button("Delete", key=f"del_{g.id}"):
                delete_goal(g.id)
                _rerun()
            if col2.button("Edit", key=f"edit_{g.id}"):
                with st.modal(f"Edit {g.name}"):
                    new_amount = st.number_input("Target amount ($)", value=g.target_amount, step=50.0)
                    new_date = st.date_input("Target date", value=g.target_date)
                    if st.button("Save changes"):
                        update_goal(g.id, new_date, new_amount)
                        _rerun()


if __name__ == "__main__":
    main()
