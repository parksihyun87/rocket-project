from __future__ import annotations
import os
from datetime import datetime, timedelta
from typing import List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

DATA_ROOT = "/opt/airflow/data/supermarket"

def data_file_path(supermarket_id: int, ds: str) ->str:
    #날짜별 파티셔닝(예: .../1/2026-03-03.csv)
    return f"{DATA_ROOT}/{supermarket_id}/{ds}.csv"

with DAG(
    dag_id = "supermarket_ingest",
    start_date = datetime(2026, 1, 1),
    schedule = None, #api 시작
    catchup = False,
    max_active_runs= 1, #DAG 런 동시 실행 제한
    max_active_tasks = 8,
    default_args = {
        "retries":1,
        "retry_delay": timedelta(minutes=1),
    }
) as dag:

    start = EmptyOperator(task_id="start")

    @task
    def read_conf() -> dict:
        """
        API로 넘긴 dag_run.conf 읽기.
        기본값도 제공.
        """
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        conf = (ctx["dag_run"].conf or {})
        supermarket_ids = conf.get("supermarket_ids", [1,2,3,4])
        wait_minues = int(conf.get("wait_minutes",20))
        return {"supermarket_ids": supermarket_ids, "wait_minutes": wait_minues}

    conf = read_conf()

    # ---슈퍼마켓별 센서: 파일이 올 때까지 기다림(폴링) ---
    # 주의 : 센서는 태스크가 "파일이 없으면 계속 기다리는" 형태
    # reschedule 모드로 워커 점유를 줄임.
    wait_1 = FileSensor(
        task_id = "wait_file_1",
        filepath =  data_file_path(1, "{{ ds }}"),
        poke_interval = 10,
        timeout = 20,
        mode = "reschedule",
        soft_fail = True,
        pool = "sensors",
    )
    wait_2 = FileSensor(
        task_id = "wait_file_2",
        filepath = data_file_path(2, "{{ ds }}"),
        poke_interval = 10,
        timeout = 20,
        mode = "reschedule",
        soft_fail = True,
        pool = "sensors",
    )
    wait_3 = FileSensor(
        task_id = "wait_file_3",
        filepath = data_file_path(3, "{{ ds }}"),
        poke_interval = 10,
        timeout = 20,
        mode = "reschedule",
        soft_fail = True,
        pool = "sensors",
    )
    wait_4 = FileSensor(
        task_id = "wait_file_4",
        filepath = data_file_path(4, "{{ ds }}"),
        poke_interval = 10,
        timeout = 20,
        mode = "reschedule",
        soft_fail = True,
        pool = "sensors",
    )

    @task(pool = "processing")
    def process_one(supermarket_id: int) -> str:
        """
        파일이 존재하면 처리 결과 파일을 만들고,
        없으면 SKIP 처리.
        반환값은 '처리된 결과 파일 경로'(xcom).
        """
        from airflow.operators.python import get_current_context
        ds = get_current_context()["ds"]

        in_path = data_file_path(supermarket_id, ds)
        out_dir = f"{DATA_ROOT}/processed/{supermarket_id}"
        out_path = f"{out_dir}/{ds}.out"

        if not os.path.exists(in_path):
            raise AirflowSkipException(f"missing input: {in_path}")

        os.makedirs(out_dir, exist_ok=True)

        # 예시 처리: 라인수만 세서 저장(진짜론 정제/집계)
        with open(in_path, "r", encoding = "utf-8") as f:
            count = sum(1 for _ in f)

        with open(out_path, "w", encoding = "utf-8") as f:
            f.write(f"supermarket_id = {supermarket_id}, ds = {ds}, rows = {count}\n")

        return out_path

    # 센서가 성공(success)이면 process가 실행되고,
    # 센서가 soft_fail로 skipped 되면 downstream은 기본 규칙상 skipped로 전파될 수 있음.
    # 그래서 process는 "센서 성공일 때만" 명확히 실행되게 연결.
    p1 = process_one.override(task_id = "process_1")(1)
    p2 = process_one.override(task_id = "process_2")(2)
    p3 = process_one.override(task_id = "process_3")(3)
    p4 = process_one.override(task_id = "process_4")(4)

    wait_1 >> p1
    wait_2 >> p2
    wait_3 >> p3
    wait_4 >> p4

    ## --- join : 일부만 와도 다음 단계로 ---
    join = EmptyOperator(
        task_id = "join_processed",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        # 실패 없어야 하고, 최소 1개는 성공해야 함( 나머지는 SKIPPED 허용)
    )

    [p1, p2, p3, p4] >> join

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def build_metrics_conf(processed_paths: List[Optional[str]]) -> dict:
        """
        처리된 파일 목록(일부 None/skip 가능)을 모아
        다음 DAG로 넘길 conf 생성.
        """
        from airflow.operators.python import get_current_context
        ds = get_current_context()["ds"]
        paths = [p for p in processed_paths if p]
        return {"ds": ds, "processed_paths": paths}

    metrics_conf = build_metrics_conf([p1, p2, p3, p4])

    # --- TriggerDagRunOperator: 다음 DAG 실행 ---
    trigger_metrics = TriggerDagRunOperator(
        task_id = "trigger_metrics_dag",
        trigger_dag_id = "supermarket_metrics",
        execution_date = "{{ logical_date }}",
        conf = metrics_conf,
        wait_for_completion = False,
        reset_dag_run = True,
    )

    start >> conf >> [wait_1, wait_2, wait_3, wait_4]
    join >> trigger_metrics
