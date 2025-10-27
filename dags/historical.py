from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector
from tasks.query import query
from tasks.filter import filter

import os
import asyncio

from airflow.sdk import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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

    mongo = MongoDBConnector(mode=os.getenv("MODE"))
    sql   = SQLDBConnector()

    @task()
    def task_query():

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
