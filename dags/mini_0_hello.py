from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id = "mini_0_hello",
    start_date = datetime(2025, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["mini"],
) as dag:
    
    @task
    def hello():
        print("hello airflow!")
        print("hello world!")
        print("airflow")
        print("notsecret_airflow")
              
    hello()


    