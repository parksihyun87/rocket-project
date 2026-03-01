from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

from rocket_project.etl.launch_dummy import make_dummy_payload, transform_payload

DATA_DIR = Path("/opt/airflow/data") # 컨테이너 안 경로

with DAG(
    dag_id = "mini_1_etl_skeleton",
    start_date= datetime(2025, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["mini", "etl"],
) as dag:
    
    @task
    def extract() -> dict:
        return make_dummy_payload()
    
    @task
    def transform(payload: dict) -> list[dict]:
        return transform_payload(payload)
    
    @task
    def load(rows: list[dict]) -> str:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out = DATA_DIR / f"launch_rows_{ts}.json"
        out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding = "utf-8")
        return str(out)
    
    # 엣지 (의존성) 연결: extract -> transform -> load
    payload = extract()
    rows = transform(payload)
    load(rows)