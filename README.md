# 🌀 Data Pipeline

Airflow–based orchestration system for managing database integration, ETL, and ML workflows.

Containerized environment is orchestrated via Docker Compose to spin up a complete Airflow cluster locally or remotely.

---

## 🚀 Quick Start

### Initial Start

```bash
docker compose up -d
```

Builds custom Airflow image (local `Dockerfile`) and starts all required services:

* **PostgreSQL** (Airflow metadata DB)
* **Redis** (Celery broker)
* **Airflow API Server**
* **Airflow Scheduler**
* **Airflow DAG Processor**
* **Airflow Worker** (Celery)

After the containers are up, the **Airflow UI** is accessible at:

```
http://localhost:8080
```

### Subsequent Workflows

If changes are made to `docker-compose.yaml`:

```bash
docker compose down -v
docker compose up -d
```

If changes are made to `Dockerfile` or `requirements.txt`:

```bash
docker compose build --no-cache
docker compose up -d
```

---

## 📁 Repository Structure

```
pipeline-airflow/
│
├── config/                     # Airflow & environment configs
│   ├── .env                    # Credentials for Navicat, MongoDB
│   ├── airflow.cfg             # Airflow base configurations
│   ├── configs.py              # Configurations for Navicat, MongoDB
│   └── ef_aliyun_pem           # Navicat SSH Key
│
├── core/                       # Shared logic and states across tasks
│
├── dags/                       # DAG definitions
│   └── recruited.py            # Main DAG orchestrating the recruited data pipeline
│
├── database/                   # Database connectors & queries
│   ├── MongoDBConnector.py     # MongoDB connector class
│   ├── SQLDBConnector.py       # MySQL connector class
│   └── queries.py              # Parameterized SQL queries
│
├── logs/                       # Airflow task logs (auto-generated)
│
├── tasks/                      # Modular task definitions executed by DAGs
│
├── utils/                      # Utility modules
│
├── docker-compose.yaml         # Orchestrates all Airflow services
│
├── Dockerfile                  # Custom Airflow image with extra dependencies
│
├── requirements.txt            # Python dependencies installed at build
│
└── README.md
```

---

## 🧩 DAG Overview

### Recruited DAG

`dags/recruited.py`

Orchestrates the tasks for processing the data from recruited patients.

**Task sequence:**

1. Query – Extract patient data and raw measurements from MySQL, MongoDB
2. Filter – Clean and validate raw measurements
3. Model – Format validated measurement data and perform feature extraction to be subsequently used for ML tasks

---

## 🧠 Development Notes

* For local testing, change the `MODE` environment variable in `docker-compose.yaml` (accepts 'local' or 'remote')

* Logs for each DAG run are stored under `/logs/dag_id=<dag_name>/`

---
