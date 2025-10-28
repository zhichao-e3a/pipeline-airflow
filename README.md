# 🌀 Data Pipeline

Airflow–based orchestration system for managing database integration, ETL, and ML workflows.

Containerized environment is orchestrated via Docker Compose to spin up a complete Airflow cluster locally or remotely.

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

### ☁️ Deployment Overview

#### Details

The entire Airflow cluster runs on a GCE virtual machine instance, provisioned with Docker Engine and Docker Compose.

Airflow can be accessed via the public endpoint:

http://35.240.213.6:8080/

#### Access to Instance

1. SSH into instance

    ```bash
    gcloud compute ssh pipeline-airflow \
      --zone=asia-southeast1-b \
      --project=tidy-computing-462809-e1
    ```

2. Pull from remote

    ```bash
    cd ~/apps/pipeline-airflow
    
    git pull origin main
    ```

3. Copy environment variable and keys to instance (run locally)

    ```bash
    # Can be checked by running `whoami` inside instance 
    GCE_USER={user}
   
    SSH_PRIVATE_PATH={path/to/ssh/private_key}
    
    PROJECT_ROOT_PATH={path/to/project_root}
   
    scp -i $SSH_PRIVATE_PATH $PROJECT_ROOT_PATH/.env <user>@35.240.213.6:~/apps/pipeline-airflow/
    
    scp -i $SSH_PRIVATE_PATH $PROJECT_ROOT_PATH/config/.env <user>@35.240.213.6:~/apps/pipeline-airflow/
    
    scp -i $SSH_PRIVATE_PATH $PROJECT_ROOT_PATH/config/ef_aliyun_pem <user>@35.240.213.6:~/apps/pipeline-airflow/
    ```

4. Append `AIRFLOW_UID` to `.env` file (run from project root)
    
    ```bash
    echo "AIRFLOW_UID=$(id -u)" >> .env
    ```

5. Create tmux session and attach to it
    
    ```bash
    tmux new -s {session-name}
    
    tmux attach -t {session-name}
    ```

6. Start containers inside session (Ctrl+B followed by D to detach from session)

    ```bash
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

`dags/backfill.py`

Refreshes and repopulates MongoDB collections.

**Task sequence:**

1. Reset - Removes all rows from MongoDB collections, resets database watermarks
2. Recruited – Extract recruited patient data and validated measurements from MySQL, MongoDB, followed by feature extraction
3. Historical – Extract historical patient data and validated measurements from MySQL, MongoDB, followed by feature extraction

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