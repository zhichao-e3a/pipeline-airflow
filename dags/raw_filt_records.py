from tasks.query import query
from tasks.filter import filter
from utils.notifier import on_task_success, on_task_failure

import os
import asyncio

from airflow.sdk import dag, task
from datetime import datetime, timedelta

from config.configs import REMOTE_MONGO_CONFIG, TEST_MONGO_CONFIG, SQL_CONFIG
mode = os.getenv("MODE")
if mode == "TEST": cfg = TEST_MONGO_CONFIG
if mode == "PROD": cfg = REMOTE_MONGO_CONFIG
from database_manager.database.mongo import MongoDBConnector
from database_manager.database.mysql import SQLDBConnector

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": on_task_failure,
    # "on_success_callback": on_task_success
}

@dag(
    dag_id="raw_filt_records",
    description="Query and filter measurements for all patients (both Recruited and Historical)",
    default_args=default_args,
    start_date=datetime(2025, 10, 22),
    # schedule="30 8 * * *",
    catchup=False,
    max_active_runs=2
)
def dag_raw_filt_records():

    @task()
    def task_query():

        print(f"=============== [{mode}] START QUERY TASK ===============")
        mongo   = MongoDBConnector(cfg)
        sql     = SQLDBConnector(SQL_CONFIG)
        asyncio.run(query(sql=sql, mongo=mongo))
        print("=============== END QUERY TASK ===============")

    @task()
    def task_filter():

        print(f"=============== [{mode}] START FILTER TASK ===============")
        mongo = MongoDBConnector(cfg)
        asyncio.run(filter(mongo=mongo))
        print("=============== END FILTER TASK ===============")

    task_query() >> task_filter()

dag = dag_raw_filt_records()