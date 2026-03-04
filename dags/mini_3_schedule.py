from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import json
import os

def calculate_stats(execution_date, next_execution_date):

    input_path = f"/opt/airflow/data/events/{execution_date}.json"
    output_path = f"/opt/airflow/data/stats/{execution_date}.csv"

    if not os.path.exists(input_path):
        return

    with open(input_path) as f:
        data = json.load(f)
    df = pd.json_normalize(data["results"])

    stats = df.groupby("name").size().reset_index(name="count")

    os.makedirs("/opt/airflow/data/stats", exist_ok=True)

    # 멱등성: 덮어쓰기
    stats.to_csv(output_path, index=False)

with DAG(
    dag_id="daily_user_stats",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag: 

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command="mkdir -p /opt/airflow/data/events && curl -o /opt/airflow/data/events/{{ ds }}.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming/'"
    )

    calculate = PythonOperator(
        task_id="calculate_stats",
        python_callable=calculate_stats,
        op_kwargs={
            "execution_date": "{{ ds }}",
            "next_execution_date": "{{ next_ds }}"
        }
    )

    fetch_events >> calculate