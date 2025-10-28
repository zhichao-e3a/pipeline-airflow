from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector
from tasks.query import query
from tasks.filter import filter
from utils.notifier import on_task_failure, on_task_success

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
    dag_id="historical",
    description="Query, filter measurements for historical patients.",
    default_args=default_args,
    start_date=datetime(2025, 10, 22),
    schedule="0 9 * * 1",
    catchup=False,
    max_active_runs=2
)
def historical_dag():

    @task()
    def task_query():

        mongo = MongoDBConnector(mode=os.getenv("MODE"))
        sql = SQLDBConnector()

        print("[1] START QUERYING DATA")

        asyncio.run(
            query(
                sql = sql,
                mongo = mongo,
                origin = "hist"
            )
        )

        print("[1] END QUERYING DATA")

    @task()
    def task_filter():

        mongo = MongoDBConnector(mode=os.getenv("MODE"))

        print("[2] START FILTERING DATA")

        asyncio.run(
            filter(
                mongo = mongo,
                origin = "hist"
            )
        )

        print("[2] END FILTERING DATA")

    task_query() >> task_filter()

dag = historical_dag()
