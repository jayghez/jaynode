# Jaynode: Personal Server-based Compute Stack

Jaynode is a self-hosted data stack on my personal node server, running:

- Apache Airflow (for Python DAGs)
- Streamlit (for visualization apps)
- PostgreSQL (shared DB instance)
- GitHub Actions self-hosted runner (auto-deploys on push)

---

## Current Ports

Streamlit App for Personal Finaces 8501:8502
Airflow UI :8081


## Quick Start

```bash
git clone https://github.com/jayghez/jaynode.git
cd jaynode
docker-compose up -d --build


# Start stack (foreground build)
docker compose up -d --build

# Stop stack
docker compose down

# View container status
docker compose ps
```



## 🤖 GitHub Actions Runner on Personal Server
Runner folder, Run manually (foreground)::
```
~/actions-runner
cd ~/actions-runner
./run.sh
```
Run as a background service:

```sudo ./svc.sh install
sudo ./svc.sh start
```

Check logs:
```
sudo journalctl -u actions.runner.jayghez.jaynode.service -f
```
## 📁 Data Storage (Postgres)
Use the same Postgres instance but a new schema:

CREATE SCHEMA airflow_data;
ayghez.jaynode







