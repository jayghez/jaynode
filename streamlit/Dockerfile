FROM python:3.10-slim
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip && \
    pip install streamlit pandas psycopg2 matplotlib seaborn

CMD streamlit run apps/dashboard.py --server.port=8501 --server.enableXsrfProtection=false
