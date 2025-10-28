from database.MongoDBConnector import MongoDBConnector
from tasks.model import model
from utils.notifier import on_task_success, on_task_failure

import os
import asyncio

from airflow.sdk import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
    "on_success_callback": on_task_success
}

@dag(
    dag_id="model",
    description="Model measurements for recruited and historical patients.",
    default_args=default_args,
    start_date=datetime(2025, 10, 22),
    schedule="0 9 * * 2,4",
    catchup=False,
    max_active_runs=2
)
def model_dag():

    @task()
    def task_model_rec():

        mongo = MongoDBConnector(mode=os.getenv("MODE"))

        print("[1] START MODELING DATA FOR REC")

        asyncio.run(
            model(
                mongo = mongo,
                origin = "rec"
            )
        )

        print("[1] END MODELING DATA FOR REC")

    @task()
    def task_model_hist():

        mongo = MongoDBConnector(mode=os.getenv("MODE"))

        print("[2] START MODELING DATA FOR HIST")

        asyncio.run(
            model(
                mongo = mongo,
                origin = "hist"
            )
        )

        print("[2] END MODELING DATA FOR HIST")

    [task_model_rec(), task_model_hist()]

dag = model_dag()
