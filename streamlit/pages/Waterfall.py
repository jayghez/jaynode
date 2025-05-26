import streamlit as st
from datetime import date
from dataclasses import dataclass
from typing import List
import psycopg2
from psycopg2.extras import DictCursor

# --- CONFIG ------------------------------------------------------------
# Re‑use the same Postgres container/params you already employ elsewhere
DB_PARAMS = dict(
    dbname="airflow",   # ↳ update if your finance app uses a different DB
    user="airflow",
    password="airflow",
    host="postgres",    # Docker‑compose service name
    port="5432",
)
TABLE_SQL = """
CREATE TABLE IF NOT EXISTS saving_goals (
    id            SERIAL PRIMARY KEY,
    name          TEXT UNIQUE NOT NULL,
    target_date   DATE        NOT NULL,
    target_amount NUMERIC     NOT NULL CHECK (target_amount >= 0)
);
"""
CATCH_ALL_NAME = "Emergency Fund"  # fixed catch‑all goal

# --- DATA MODEL --------------------------------------------------------
@dataclass
class Goal:
    id: int | None
    name: str
    target_date: date
    target_amount: float
    allocation: float = 0.0  # calculated – not persisted

# --- DB HELPERS --------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_PARAMS, cursor_factory=DictCursor)

def ensure_table_and_seed():
    """Create table & default Emergency Fund row if missing."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(TABLE_SQL)
        conn.commit()
        # Seed Emergency Fund if absent
        cur.execute("SELECT 1 FROM saving_goals WHERE lower(name) = lower(%s) LIMIT 1", (CATCH_ALL_NAME,))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO saving_goals (name, target_date, target_amount) VALUES (%s, %s, %s)",
                (CATCH_ALL_NAME, date(2100, 1, 1), 15000),
            )
            conn.commit()

@st.cache_data(show_spinner=False)
def fetch_goals() -> List[Goal]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, name, target_date, target_amount FROM saving_goals")
        rows = cur.fetchall()
    return [Goal(r[0], r[1], r[2], float(r[3])) for r in rows]


def upsert_goal(goal: Goal):
    with get_conn() as conn, conn.cursor() as cur:
        if goal.id is None:
            cur.execute(
                """
                INSERT INTO saving_goals (name, target_date, target_amount)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (goal.name, goal.target_date, goal.target_amount),
            )
            goal.id = cur.fetchone()[0]
        else:
            cur.execute(
                "UPDATE saving_goals SET target_date = %s, target_amount = %s WHERE id = %s",
                (goal.target_date, goal.target_amount, goal.id),
            )
        conn.commit()


def delete_goal(goal_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM saving_goals WHERE id = %s", (goal_id,))
        conn.commit()

# --- ALLOCATION LOGIC (waterfall) -------------------------------------

def allocate_cash(total_balance: float, goals: List[Goal]) -> List[Goal]:
    catch_all = next((g for g in goals if g.name.lower() == CATCH_ALL_NAME.lower()), None)
    others    = [g for g in goals if g is not catch_all]
    others.sort(key=lambda g: g.target_date)  # chronological

    remaining = total_balance
    for g in others:
        need         = max(g.target_amount - g.allocation, 0)
        g.allocation = min(need, remaining)
        remaining   -= g.allocation

    if catch_all:  # what’s left → Emergency Fund
        catch_all.allocation = remaining
        ordered = [catch_all] + others
    else:
        ordered = others
    return ordered

# --- UI ----------------------------------------------------------------

def main():
    st.set_page_config(page_title="Savings Goals", page_icon="💸")
    st.title("Envelope‑style Savings Goals")

    ensure_table_and_seed()

    # 1) Account balance
    total_balance = st.number_input("Current savings account balance ($)", min_value=0.0, step=100.0, value=10_000.0, format="%0.2f")

    # 2) Goal creation form
    with st.expander("➕ Add a new goal"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name = st.text_input("Goal name")
        with col2:
            tgt_date = st.date_input("Target date", value=date.today().replace(year=date.today().year + 1))
        with col3:
            amount = st.number_input("Target amount ($)", min_value=0.0, step=50.0, format="%0.2f")
        if st.button("Add goal"):
            if name and name.lower() != CATCH_ALL_NAME.lower():
                goal = Goal(id=None, name=name.strip(), target_date=tgt_date, target_amount=amount)
                upsert_goal(goal)
                st.success(f"Added goal '{name}'.")
                st.experimental_rerun()
            else:
                st.warning("Name must be unique and not 'Emergency Fund'.")

    # 3) Fetch + allocate
    goals = fetch_goals()
    goals = allocate_cash(total_balance, goals)

    # 4) Display goals
    for g in goals:
        st.subheader(f"{g.name} — ${g.allocation:,.0f} / ${g.target_amount:,.0f}")
        st.progress(min(g.allocation / g.target_amount, 1.0))

        if g.name.lower() != CATCH_ALL_NAME.lower():
            edit_col, del_col = st.columns(2)
            with edit_col:
                if st.button("✏️ Edit", key=f"edit_{g.id}"):
                    with st.modal(f"Edit {g.name}"):
                        new_amount = st.number_input("Target amount ($)", value=g.target_amount, step=50.0, format="%0.2f")
                        new_date   = st.date_input("Target date", value=g.target_date)
                        if st.button("Save changes"):
                            g.target_amount = new_amount
                            g.target_date   = new_date
                            upsert_goal(g)
                            st.experimental_rerun()
            with del_col:
                if st.button("🗑️ Delete", key=f"del_{g.id}"):
                    delete_goal(g.id)
                    st.experimental_rerun()

if __name__ == "__main__":
    main()
