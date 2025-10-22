from database.MongoDBConnector import MongoDBConnector
from database.SQLDBConnector import SQLDBConnector
from tasks.query import query
from tasks.filter import filter

import os
import asyncio

from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="recruited",
    description="Query, filter measurements for recruited patients.",
    default_args=default_args,
    start_date=datetime(2025, 10, 22),
    schedule="30 8 * * *",
    catchup=False
)
def recruited_dag():

    mongo = MongoDBConnector(mode=os.getenv("MODE"))
    sql   = SQLDBConnector()

    @task()
    def task_query():

        print("[1] START QUERYING DATA")

        asyncio.run(
            query(
                sql = sql,
                mongo = mongo,
                origin = "rec"
            )
        )

        print("[1] END QUERYING DATA")

    @task()
    def task_filter():

        print("[2] START FILTERING DATA")

        asyncio.run(
            filter(
                mongo = mongo,
                origin = "rec"
            )
        )

        print("[2] END FILTERING DATA")

    task_query() >> task_filter()

dag = recruited_dag()
