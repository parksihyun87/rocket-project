from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import gzip
import os
import json
import pandas as pd
import logging

DATA_DIR = "/opt/airflow/data/wiki"
PAGES= ["Google", "Amazon", "Apple", "Microsoft", "Facebook"]
PROJECT = "en"

def extract_counts(ds: str, ds_nodash: str, hour: str):
    """
    ds: 'YYYY-MM-DD' 문자열 (Jinja로 렌더링이 되어 들어옴)
    ds_nodash: YYYYMMDD
    hour: HH
    """
    gz_path = f"{DATA_DIR}/pageviews-{ds_nodash}-{hour}0000.gz"
    sql_path = f"{DATA_DIR}/upsert_pageviews_{ds_nodash}_{hour}.sql"

    if not os.path.exists(gz_path):
        raise AirflowSkipException(f"Missing input file: {gz_path}")

    counts = {p: 0 for p in PAGES}

    with gzip.open(gz_path, "rt", encoding = "utf-8", errors = "ignore") as f:
        for line in f:
            # 포맷: project title views bytes
            parts = line.split()
            if len(parts) < 4:
                continue
            project, title, views = parts[0], parts[1], parts[2]

            if project == PROJECT and title in counts:
                try:
                    counts[title] += int(views)
                except ValueError:
                    pass
    os.makedirs(DATA_DIR, exist_ok=True)

    # PostrgesOperator가 실행할 SQL 파일 생성(UPSERT)
    # 테이블 PK: (ds, hour, title)
    # ON CONFLIC로 중복 실행(재시도/백필)에도 결과 안정(멱등성)
    lines= []
    lines.append("-- auto genertaed")
    for title, cnt in counts.items():
        safe_title = title.replace("'","''")
        lines.append(
            "INSERT INTO pageview_counts(ds, hour, title, view_count) "
            f"VALUES('{ds}', {int(hour)}, '{safe_title}', {cnt}) "
            "ON CONFLICT (ds, hour, title) DO UPDATE "
            "SET view_count = EXCLUDED.view_count;"
        )

    with open(sql_path, "w", encoding = "utf-8") as out:
        out.write("\n".join(lines) + "\n")

with DAG(
    dag_id = "wikipedia_pageviews_to_postgres",
    start_date=datetime(2025, 1, 15),
    end_date=datetime(2025, 1, 16),   # start_date 하루 뒤
    schedule = "@daily",
    catchup = True,
) as dag:

    # (1) 테이블 생성 (한 번만 해도 되지만, IF NOT EXISTS라 매번 돌려도 안전)
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id= "postgres_default",
        sql= """
        CREATE TABLE IF NOT EXISTS pageview_counts (
        ds DATE NOT NULL,
        hour INT NOT NULL,
        title TEXT NOT NULL,
        view_count BIGINT NOT NULL,
        PRIMARY KEY (ds, hour, title)
        );
        """,
    )

    # (2) 다운로드: 실행날짜(ds) 기반으로 URL/파일명 만들기 (Jinja)
    # 여기선 "00시" 파일만 예시로 받음. (원하면 hourly로 확장 가능)
    download = BashOperator(
        task_id = "download_pageviews",
        bash_command = (
            "mkdir -p /opt/airflow/data/wiki && "
            "curl -fsSL -o /opt/airflow/data/wiki/pageviews-{{ ds_nodash }}-000000.gz "
            "https://dumps.wikimedia.org/other/pageviews/{{ ds[:4]}}/{{ ds[:7] }}/"
            "pageviews-{{ ds_nodash }}-000000.gz"
        ),
    )

    #(3) gzip 읽어서 관심 페이지 집계 + upsert SQL 파일 생성
    extract = PythonOperator(
        task_id = "extract_counts",
        python_callable = extract_counts,
        op_kwargs = {
            "ds": "{{ ds }}",
            "ds_nodash": "{{ ds_nodash }}",
            "hour": "00",
        }
    )

    # (4) Postgres 적재: (3)에서 만든 SQL 파일 실행
    def load_sql(ds_nodash, **context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        sql_path = f"/opt/airflow/data/wiki/upsert_pageviews_{ds_nodash}_00.sql"
        with open(sql_path) as f:
            sql = f.read()
        PostgresHook(postgres_conn_id="postgres_default").run(sql)

    load_to_postgres = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = load_sql,
        op_kwargs = {"ds_nodash": "{{ ds_nodash }}"},
    )

    create_table >> download >> extract >> load_to_postgres
