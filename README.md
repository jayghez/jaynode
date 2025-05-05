# jaynode
personal compute node

This repo runs Python jobs (Airflow) and Streamlit apps on a local server using Docker Compose.

## Services

- `airflow` – Runs job DAGs :8080
- `streamlit` – Dashboard :8501

## Quick Start

```bash
git clone https://github.com/jayghez/jaynode.git
cd jaynode
docker-compose up -d --build