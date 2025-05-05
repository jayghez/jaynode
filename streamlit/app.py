import streamlit as st

st.title("Umbrel Streamlit App")
st.write("This app is running on your Umbrel box!")
number = st.slider("Pick a number", 0, 100, 42)
st.write("You picked:", number)