# 🌀 Data Pipeline

Airflow–based orchestration system for managing database integration, ETL, and ML workflows.

Containerized environment is orchestrated via Docker Compose to spin up a complete Airflow cluster locally or remotely.

---

### ☁️ Deployment Overview

The entire Airflow cluster runs on a GCE virtual machine instance, provisioned with Docker Engine and Docker Compose.

Airflow can be accessed via the public endpoint:

http://35.240.213.6:8080/

---

### 🧠 Development Notes

* For local testing, change the `MODE` environment variable in `docker-compose.yaml` (accepts 'local' or 'remote')

* Logs for each DAG run are stored under `/logs/dag_id=<dag_name>/`

* Before starting up containers in VM, need to copy `.env`, `config/.env`, `ef_aliyun_pem` from local to remote VM 

---

### 🚀 Quick Start (Local)

#### Initial Start

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

#### Subsequent Workflows

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

### 🧩 DAG Overview

`dags/recruited.py`, `dags/historical.py`

Orchestrates the tasks for processing the data from recruited, historical patients.

**Task sequence:**

1. Query – Extract patient data and raw measurements from MySQL, MongoDB
2. Filter – Clean and validate raw measurements

`dags/model.py`

Formats validated measurement data and perform feature extraction to be subsequently used for ML tasks.

**Task sequence (executed in parallel):**

1. Recruited – Extract recruited patient data and validated measurements from MySQL, MongoDB, followed by feature extraction
2. Historical – Extract historical patient data and validated measurements from MySQL, MongoDB, followed by feature extraction

---

### ⏰ Scheduling & Automation

🗓️ `dags/recruited.py` Daily 08:30AM

🗓️ `dags/historical.py` Weekly (Mon) 09:00AM

🗓️ `dags/model.py` Weekly (Tue, Thu) 09:00AM

Airflow automates all data workflows through scheduled DAG runs.

All DAGs operate under Asia/Singapore timezone (defined in `airflow.cfg`).

---

### 📁 Repository Structure

```
pipeline-airflow/
│
├── config/                     # Airflow & environment configs
│   ├── .env                    # Credentials for Navicat, MongoDB (ignored)
│   ├── airflow.cfg             # Airflow base configurations
│   ├── configs.py              # Configurations for Navicat, MongoDB
│   └── ef_aliyun_pem           # Navicat SSH Key (ignored)
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
├── .env                        # Airflow User credentials, UID (ignored)
│
└── README.md
```

---