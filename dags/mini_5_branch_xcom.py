from __future__ import annotations

import json
import os
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

#경로 
DATA_DIR= "/opt/airflow/data/rocket"
EVENTS_DIR = f"{DATA_DIR}/events"
IMAGES_DIR = f"{DATA_DIR}/images"

def extract_image_urls(ds: str, ti, **_):
    """
    xcom push 다운로드된 JSON에서 이미지 URL만 뽑아 xcom에 저장
    """
    input_path = f"{EVENTS_DIR}/{ds}.json"
    if not os.path.exists(input_path):
        raise AirflowSkipException(f"Missing input json: {input_path}")

    with open(input_path, "r", encoding = "utf-8") as f:
        data = json.load(f)

    results = data.get("results", [])
    urls: List[str]  = []
    for item in results:
        url = item.get("image")
        if isinstance(url, str) and url.startswith("http"):
            urls.append(url)
    #너무 많이 받지 말고 상위 N개만 
    urls = urls[:5]

    ti.xcom_push(key="image_urls", value = urls)

def choose_download_path(ti, **_):
    """
    branch
    이미지 url이 있으면 download_images로
    없으면 no_images로
    """
    urls = ti.xcom_pull(task_ids="extract_image_urls", key="image_urls") or []
    if len(urls) > 0:
        return "download_images"
    return "no_images"

def download_images_from_xcom(ds: str, ti, **_):
    """
    xcom pull-> 실제 다운-> xcom push
    extract_image_urls가 push 한 url 리스트를 pull 해서 이미지 파일로 저장
    """
    urls= ti.xcom_pull(task_ids="extract_image_urls", key = "image_urls") or []
    if not urls:
        raise AirflowSkipException("No image urls")
    
    os.makedirs(IMAGES_DIR, exist_ok = True)

    saved: List[str] = []
    # 리퀘스트 대신 컬을 스고 싶으면 배시오퍼레이터로 바꿔도 됨
    # 여기선 파이썬 내에서 간단히 처리(urllib)
    import urllib.request

    for i, url in enumerate(urls, start=1):
        out_path = f"{IMAGES_DIR}/{ds}_{i}.jpg"
        try:
            urllib.request.urlretrieve(url, out_path)
            saved.append(out_path)
        except Exception:
            #일부 실패도 전체 실패되려면 raise로 교체
            continue
        
    if not saved:
        raise AirflowSkipException("All downloads failed or no files saved")
    
    ti.xcom_push(key="saved_images", value = saved)

def notify_result(ds: str, ti, **_):
    """
    최신 run에서만 실행(LatestOnlyOperator 뒤)
    결과를 요약 출력(실전은 Slack/Email로 교체)
    """
    urls = ti.xcom_pull(task_ids="extract_image_urls", key= "image_urls") or []
    saved = ti.xcom_pull(task_ids= "download_images", key = "saved_images") or []

    print("===notify===")
    print(f"ds={ds}")
    print(f"image_urls_count={len(urls)}")
    print(f"saved_image_count={len(saved)}")
    if saved:
        print("saved_images:")
        for p in saved:
            print(" -", p)

with DAG(
    dag_id = "rocket_launches_with_branch_xcom_trigger",
    start_date = datetime(2026, 2, 28),
    schedule = "@daily",
    catchup = True
) as dag:
    start = EmptyOperator(task_id = "start")

    #1 JSON 다운로드 (템플릿 : ds로 날짜별 저장)
    fetch_events = BashOperator(
        task_id = "fetch_events",
        bash_command = (
            f"mkdir -p {EVENTS_DIR} && "
            "curl -sS 'https://ll.thespacedevs.com/2.0.0/launch/?net__gte={{ ds }}&net__lt={{ next_ds }}' "
            f"-o {EVENTS_DIR}/{{{{ ds }}}}.json"
        ),
    )

    #2 JSON에서 이미지 URL 추출 ->Xcom push
    extract = PythonOperator(
        task_id = "extract_image_urls",
        python_callable = extract_image_urls,
        op_kwargs = {"ds": "{{ ds }}"},
    )

    #3 브랜치 download images vs no images
    branch = BranchPythonOperator(
        task_id = "branch_on_images",
        python_callable= choose_download_path,
    )

    # 4 이미지 다운로드(선택된 경우)->xcom push(saved_images)
    download_images= PythonOperator(
        task_id = "download_images",
        python_callable = download_images_from_xcom,
        op_kwargs = {"ds": "{{ ds }}"},
        retries = 1,
    )

    #4-B 이미지 없으면 이 태스크만 실행(나머지 스킵)
    no_images = EmptyOperator(task_id = "no_images")

    #5 브랜치 합치기(join) 브랜치 특성상 skip이 생기므로 trigger_rule을 바꿔야 함
    join = EmptyOperator(
        task_id= "join",
        trigger_rule= "none_failed_min_one_success",
    )

    #6 최신 run에서만 notify 실행
    latest_only = LatestOnlyOperator(task_id= "latest_only")

    #7 알림(요약 출력)
    notify = PythonOperator(
        task_id = "notify",
        python_callable = notify_result,
        op_kwargs = {"ds": "{{ ds }}"},
    )

    # 의존성 구성(책 그림 형태)
    start >> fetch_events >> extract >> branch
    branch >> download_images >> join
    branch  >> no_images >> join
    join >> latest_only >> notify

