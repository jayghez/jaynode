import streamlit as st
import pandas as pd
from finance_utils import normalize, save_to_db, load_recent

st.title("💸 Personal Finance Uploader and Table Viewer")

uploaded_file = st.file_uploader("Upload a CSV statement", type="csv")
source = st.selectbox("Bank source", ["USAA", "Chase", "Apple", "Amex", "Frost", "Pre-merged Union"])

if uploaded_file:
    df_raw = pd.read_csv(uploaded_file)
    df_normalized = normalize(df_raw, source)
    st.subheader("Preview")
    st.dataframe(df_normalized.head())

    if st.button("Save to Postgres"):
        save_to_db(df_normalized)
        st.success("Saved to DB!")

st.subheader("📊 Latest Transactions")
st.dataframe(load_recent())