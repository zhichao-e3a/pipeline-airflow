from tasks.rebuild import rebuild
from utils.notifier import on_task_failure, on_task_success

import os
import asyncio

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

from config.configs import REMOTE_MONGO_CONFIG, TEST_MONGO_CONFIG
mode = os.getenv("MODE")
if mode == "TEST": cfg = TEST_MONGO_CONFIG
if mode == "PROD": cfg = REMOTE_MONGO_CONFIG
from database_manager.database.mongo import MongoDBConnector

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": on_task_failure,
    # "on_success_callback": on_task_success
}

@dag(
    dag_id="rebuild",
    description="Rebuild Mongo collections and repopulate",
    default_args=default_args,
    start_date=datetime(2025, 10, 22),
    catchup=False
)
def dag_rebuild():

    @task()
    def task_rebuild():

        print(f"=============== [{mode}] START REBUILD COLLECTIONS ===============")
        mongo = MongoDBConnector(cfg)
        asyncio.run(rebuild(mongo=mongo))
        print("=============== END REBUILD COLLECTIONS ===============")

    trigger_downstream = TriggerDagRunOperator(
        task_id="rebuild_downstream",
        trigger_dag_id="raw_filt_records",
        poke_interval=10,
        wait_for_completion=True,
        reset_dag_run=False,
        logical_date="{{ ts }}"
    )

    task_rebuild() >> trigger_downstream

dag = dag_rebuild()