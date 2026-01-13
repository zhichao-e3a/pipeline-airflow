from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(

    dag_id          = "dag_scheduled",
    start_date      = datetime(2025, 1, 1),
    schedule        = "0 * * * *",
    catchup         = False,
    max_active_runs = 1,
    default_args    = {"retries": 1, "retry_delay": timedelta(minutes=2)},

) as dag:

    trigger_query = TriggerDagRunOperator(
        task_id="trigger_dag_query",
        trigger_dag_id="dag_query",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False
    )

    trigger_filt = TriggerDagRunOperator(
        task_id="trigger_dag_filter",
        trigger_dag_id="dag_filter",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False
    )

    trigger_query >> trigger_filt