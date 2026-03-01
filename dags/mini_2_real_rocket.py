import json
import pathlib
from datetime import timedelta

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "download_rocket_launches",
    start_date = airflow.utils.dates.days_ago(14),
    schedule_interval = None,
)

download_launches=BashOperator(
    task_id = "download_launches",
    bash_command = "curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming/'", dag=dag,
)

def _get_pictures():
    #경로가 존재하는지 확인
    pathlib.Path("/opt/airflow/data/images").mkdir(parents=True, exist_ok=True)

    #launces.json 파일에 있는 모든 그림 파일을 다운로드
    try:
        with open("/opt/airflow/data/launches.json") as f:
            launches=json.load(f)
    except FileNotFoundError:
        print("launches.json file not found. Skipping image download.")
        return

    image_urls=[launch.get("image") for launch in launches["results"] if launch.get("image")]
    for image_url in image_urls:
        try:
            response = requests.get(image_url, timeout= 10)
            response.raise_for_status()
            image_filename=image_url.split("/")[-1]
            target_file=f"/opt/airflow/data/images/{image_filename}"
            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {image_url} to {target_file}")
        except requests_exceptions.MissingSchema:
            print(f"{image_url} appears to be an invalid URL.")
        except requests_exceptions.ConnectionError:
            print(f"Could not connect to {image_url}.")
        except requests_exceptions.HTTPError as e:
            print(f"HTTP error for {image_url}: {e}")
        except requests_exceptions.Timeout:
            print(f"Timeout while downloading {image_url}.")
        except requests_exceptions.RequestException as e:
            print(f"Failed to download {image_url}: {e}")

get_pictures= PythonOperator(
    task_id = "get_pictures",
    python_callable = _get_pictures,
    dag = dag,
    retries =3,
    retry_delay = timedelta(seconds=10)
)


notify = BashOperator(
    task_id = "notify",
    bash_command = 'echo "there are now $(ls /opt/airflow/data/images/ | wc -l) images."',
    dag = dag,
)

download_launches >> get_pictures >> notify
