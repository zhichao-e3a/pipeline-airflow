from core.notifier import DagNotifier

import os
import traceback
from typing import Any, Dict, Optional
from datetime import timedelta

RECIPIENTS = [
    "zhichao.lin@e3ahealth.com",
    "norine.chen@e3ahealth.com",
    "joey.hu@e3ahealth.com"
]

_LABEL_WIDTH = max(len(k) + 2 for k in ["DAG", "Task", "Run ID", "Start", "End", "Duration", "Try", "Task Log"])

def _fmt_td(td: Optional[timedelta]) -> str:

    if not td:
        return "N/A"

    total = int(td.total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)

    return f"{h:02d}:{m:02d}:{s:02d}"

def _safe(obj: Any, default: str = "N/A") -> str:

    try:
        if obj is None:
            return default
        return str(obj).strip()

    except Exception as e:
        print(e)
        return default

def _format_block(title: str, rows: list[str]) -> str:

    border = "=" * 72
    lines = [border, f"{title}", border]
    lines.extend(rows)

    return "\n".join(lines)

def _common_context(context: Dict[str, Any]) -> Dict[str, Any]:

    ti          = context.get("ti") or context.get("task_instance")
    task        = context.get("task")
    dag_run     = context.get("dag_run")
    dag         = context.get("dag")

    start       = getattr(ti, "start_date", None)
    end         = getattr(ti, "end_date", None)
    try_num     = getattr(ti, "try_number", None)
    max_tries   = getattr(task, "retries", None)

    duration    = (end - start) if (start and end) else None

    return {
        "dag_id"            : getattr(ti, "dag_id", getattr(dag, "dag_id", "N/A")),
        "task_id"           : getattr(ti, "task_id", "N/A"),
        "run_id"            : getattr(dag_run, "run_id", context.get("run_id", "N/A")),
        "start"             : start.astimezone(os.getenv("AIRFLOW_LOCAL_TZ")).strftime("%Y-%m-%d %H:%M:%S"),
        "end"               : end.astimezone(os.getenv("AIRFLOW_LOCAL_TZ")).strftime("%Y-%m-%d %H:%M:%S"),
        "duration"          : duration,
        "try_number"        : try_num,
        "max_tries"         : max_tries,
    }

def on_task_failure(context: Dict[str, Any]) -> None:

    nn = DagNotifier(
        os.getenv("EM_USER"),
        os.getenv("EM_PASS")
    )

    c = _common_context(context)

    exc: Optional[BaseException] = context.get("exception")
    exc_type = type(exc).__name__ if exc else "N/A"
    exc_text = "".join(traceback.format_exception_only(type(exc), exc)).strip() if exc else "N/A"

    header = _format_block(
        title=f"❌ Airflow Task Failure — {c['dag_id']}.{c['task_id']}",
        rows=[
            f"DAG: {c['dag_id']}",
            f"Task: {c['task_id']}",
            f"Run ID: {c['run_id']}",
            f"Start: {c['start']}",
            f"End: {c['end']}",
            f"Duration: {_fmt_td(c['duration'])}",
            f"Try: {c['try_number']} / {c['max_tries']}",
        ]
    )

    error = _format_block(
        title=f"💥 Exception ({exc_type})",
        rows=[f"Detail: {exc_text}"]
    )

    body    = f"{header}\n\n{error}\n"
    subject = f"[AIRFLOW][FAIL] {c['dag_id']}.{c['task_id']} (run_id={c['run_id']})"

    nn.email_notification(
        subject=subject,
        content=body,
        to_address=RECIPIENTS,
    )

def on_task_success(

        context: Dict[str, Any]

) -> None:

    nn = DagNotifier(
        os.getenv("EM_USER"),
        os.getenv("EM_PASS")
    )

    c = _common_context(context)

    header = _format_block(
        title=f"✅ Airflow Task Success — {c['dag_id']}.{c['task_id']}",
        rows=[
            f"DAG: {c['dag_id']}",
            f"Task: {c['task_id']}",
            f"Run ID: {c['run_id']}",
            f"Start: {c['start']}",
            f"End: {c['end']}",
            f"Duration: {_fmt_td(c['duration'])}",
            f"Try: {c['try_number']} / {c['max_tries']}",
        ]
    )

    body = f"{header}\n"
    subject = f"[AIRFLOW][OK] {c['dag_id']}.{c['task_id']} (run_id={c['run_id']})"

    nn.email_notification(
        subject=subject,
        content=body,
        to_address=RECIPIENTS
    )
