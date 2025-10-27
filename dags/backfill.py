from database.MongoDBConnector import MongoDBConnector
from tasks.backfill import backfill

import os
import asyncio

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="backfill",
    description="Reset Mongo collections and repopulate.",
    default_args=default_args,
    start_date=datetime(2025, 10, 22),
    catchup=False
)
def backfill_dag():

    mongo = MongoDBConnector(mode=os.getenv("MODE"))

    @task()
    def task_backfill():

        print("[1] START CLEARING DATA")

        asyncio.run(
            backfill(
                mongo = mongo
            )
        )

        print("[1] END CLEARING DATA")

    trigger_rec_downstream = TriggerDagRunOperator(
        task_id="rec_downstream",
        trigger_dag_id="recruited",
        poke_interval=10,
        wait_for_completion=True,
        reset_dag_run=False,
        logical_date="{{ ts }}"
    )

    trigger_hist_downstream = TriggerDagRunOperator(
        task_id="hist_downstream",
        trigger_dag_id="historical",
        poke_interval=10,
        wait_for_completion=True,
        reset_dag_run=False,
        logical_date="{{ ts }}"
    )

    task_backfill() >> trigger_rec_downstream >> trigger_hist_downstream

dag = backfill_dag()
