from core.common import job_done

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor

with DAG(

    dag_id       = "dag_merge",
    start_date   = datetime(2025, 1, 1),
    schedule     = None,
    catchup      = False,
    default_args = {"retries": 1, "retry_delay": timedelta(minutes=2)},

) as dag:

    trigger_merge = HttpOperator(
        task_id         = "start_task_merge",
        http_conn_id    = "pipeline_api",
        endpoint        = "/pipeline/merge",
        method          = "POST",
        headers         = {"Content-Type": "application/json"},
        response_check  = lambda r: r.status_code == 202,
        response_filter = lambda r: r.headers.get("Location"),
        do_xcom_push    = True
    )

    wait_merge = HttpSensor(
        task_id         = "wait_task_merge",
        http_conn_id    = "pipeline_api",
        endpoint        = "{{ ti.xcom_pull(task_ids='start_task_merge') }}",
        method          = "GET",
        response_check  = job_done,
        poke_interval   = 30,
        timeout         = 60*60,
        mode            = "reschedule"
    )

    trigger_merge >> wait_merge