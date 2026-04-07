FROM apache/airflow:2.9.3
RUN pip install --no-cache-dir "sagemaker[local]>=2.0,<3.0" torch
