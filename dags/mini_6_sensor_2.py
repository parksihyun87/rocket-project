from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id = "supermarket_metrics",
    start_date = datetime(2026, 1, 1),
    schedule = None, #trigger로만 실행
    catchup = False,
    max_active_runs= 1, #DAG 런 동시 실행 제한
    default_args = {
        "retries":0
    },
) as dag:
    # 옵션- ingest DAG의 join_processed 가 끝났는지 기다리기 예시
    # 보통은 trigger로 충분하지만, 책에 나오는 ExternalTaskSensor를 포함하려고 넣음
    wait_ingest = ExternalTaskSensor(
        task_id = "wait_ingest_join",
        external_dag_id = "supermarket_ingest",
        external_task_id = "join_processed",
        allowed_states = ["success", "skipped"], #일부만 처리되어도 join이 success로 끝나게 했음
        failed_states = ["failed"],
        mode = "reschedule",
        poke_interval = 30,
        timeout = 60 *10,
    )

    @task
    def read_conf() -> dict:
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        return ctx["dag_run"].conf or {}

    @task
    def create_metrics(run_conf: dict):
        ds = run_conf.get("ds")
        paths = run_conf.get("processed_paths", [])
        print("== METRICS ==")
        print("ds:", ds)
        print("processed_paths:", paths)

    c = read_conf()
    wait_ingest >> c  # wait_ingest 완료 후 read_conf 실행
    create_metrics(c)
