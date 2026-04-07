from __future__ import annotations

import gzip
import io
import os
import struct
import tarfile
import tempfile
from datetime import datetime

import boto3
import sagemaker
import torch
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sagemaker.pytorch import PyTorch

AWS_CONN_ID = "aws_default"
REGION_NAME = "ap-northeast-2"

RAW_BUCKET = os.environ.get("MNIST_RAW_BUCKET", "your-mnist-data-bucket")
MODEL_BUCKET = os.environ.get("MNIST_MODEL_BUCKET", "your-mnist-model-bucket")

RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"
TRAINING_OUTPUT_PREFIX = "training-output"

LOCAL_MODEL_DIR = "/opt/airflow/data/models"

# sagemake arn
SAGEMAKER_ROLE_ARN = os.environ.get("SAGEMAKER_ROLE_ARN")

def _make_boto3_session_from_airflow(conn_id: str) -> boto3.Session:
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    return boto3.Session(
        aws_access_key_id = conn.login,
        aws_secret_access_key = conn.password,
        aws_session_token = extra.get("aws_session_token"),
        region_name = extra.get("region_name") or extra.get("region") or REGION_NAME,
    )

def _read_idx_images(gz_bytes: bytes) -> torch.Tensor:
    with gzip.GzipFile(fileobj = io.BytesIO(gz_bytes)) as f:
        raw = f.read()

    magic, num_images, rows, cols = struct.unpack(">IIII", raw[:16])
    if magic != 2051:
        raise ValueError(f"Invalid image magic number: {magic}")
    
    data = torch.frombuffer(raw[16:], dtype = torch.uint8).clone()
    return data.view(num_images, 1, rows, cols).float() /255.0

def _read_idx_labels(gz_bytes: bytes) -> torch.Tensor:
    with gzip.GzipFile(fileobj = io.BytesIO(gz_bytes)) as f:
        raw = f.read()

    magic, num_labels = struct.unpack(">II", raw[:8])
    if magic != 2049:
        raise ValueError(f"Invalid label magic number: {magic}")
    
    return torch.frombuffer(raw[8:], dtype = torch.uint8).clone().long()

@dag(
    dag_id = "mnist_sagemaker_lenet_local",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    max_active_runs = 1,
    default_args = {"retries": 0},
)

def mnist_sagemaker_lenet_local():
    @task
    def verify_raw_files() -> dict:
        s3 = S3Hook(aws_conn_id = AWS_CONN_ID)

        required = [
            f"{RAW_PREFIX}/train-images-idx3-ubyte.gz",
            f"{RAW_PREFIX}/train-labels-idx1-ubyte.gz",
            f"{RAW_PREFIX}/t10k-images-idx3-ubyte.gz",
            f"{RAW_PREFIX}/t10k-labels-idx1-ubyte.gz",
        ]

        missing = [key for key in required if not s3.check_for_key(key=key, bucket_name = RAW_BUCKET)]
        if missing:
            raise ValueError(f"Missing raw files in S3: {missing}")
        
        return {"ok": True}
    
    @task
    def preprocess_to_pt(_: dict) -> dict:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        ds_nodash = ctx["ds_nodash"]
        s3 = S3Hook(aws_conn_id = AWS_CONN_ID)

        def get_bytes(key: str) -> bytes:
            obj = s3.get_key(key=key, bucket_name= RAW_BUCKET)
            if obj is None:
                raise ValueError(f"Could not load S3 object: {key}")
            return obj.get()["Body"].read()
        train_images = _read_idx_images(get_bytes(f"{RAW_PREFIX}/train-images-idx3-ubyte.gz"))
        train_labels = _read_idx_labels(get_bytes(f"{RAW_PREFIX}/train-labels-idx1-ubyte.gz"))
        test_images = _read_idx_images(get_bytes(f"{RAW_PREFIX}/t10k-images-idx3-ubyte.gz"))
        test_labels = _read_idx_labels(get_bytes(f"{RAW_PREFIX}/t10k-labels-idx1-ubyte.gz"))

        with tempfile.TemporaryDirectory() as tmpdir:
            train_path = os.path.join(tmpdir, "train.pt")
            test_path = os.path.join(tmpdir, "test.pt")

            torch.save({"images": train_images, "labels": train_labels}, train_path)
            torch.save({"images": test_images, "labels": test_labels}, test_path)

            train_key = f"{PROCESSED_PREFIX}/{ds_nodash}/train/train.pt"
            test_key = f"{PROCESSED_PREFIX}/{ds_nodash}/test/test.pt"

            s3.load_file(train_path, train_key, RAW_BUCKET, replace = True)
            s3.load_file(test_path, test_key, RAW_BUCKET, replace = True)

        return {
            "train_s3_uri": f"s3://{RAW_BUCKET}/{PROCESSED_PREFIX}/{ds_nodash}/train",
            "test_s3_uri": f"s3://{RAW_BUCKET}/{PROCESSED_PREFIX}/{ds_nodash}/test",
            "output_s3_uri": f"s3://{RAW_BUCKET}/{PROCESSED_PREFIX}/{ds_nodash}/",
        }

    @task
    def start_training(processed_info: dict) -> str:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        ds_nodash = ctx["ds_nodash"]

        boto_session = _make_boto3_session_from_airflow(AWS_CONN_ID)
        sm_session = sagemaker.Session(boto_session= boto_session)

        from datetime import datetime, timezone
        job_name = f"mnist-lenet5-{ds_nodash}-{datetime.now(timezone.utc).strftime('%H%M%S')}"

        estimator = PyTorch(
            entry_point = "train.py",
            source_dir = "/opt/airflow/src/sagemaker_mnist",
            role = SAGEMAKER_ROLE_ARN,
            framework_version= "2.1",
            py_version = "py310",
            instance_type = "ml.m5.large",
            instance_count = 1,
            output_path = processed_info["output_s3_uri"],
            sagemaker_session = sm_session,
            base_job_name =job_name,
            hyperparameters = {
                "epochs": 3,
                "batch-size": 128,
                "lr": 0.001,
            },
        )

        estimator.fit(
            inputs = {
                "train": processed_info["train_s3_uri"],
                "test": processed_info["test_s3_uri"],
            },
            wait = True,
            logs = True,
            job_name = job_name,
        )

        desc = sm_session.sagemaker_client.describe_training_job(TrainingJobName = job_name)
        return desc["ModelArtifacts"]["S3ModelArtifacts"]

    @task
    def download_and_extract_model(model_s3_uri: str) -> str:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        ds_nodash = ctx["ds_nodash"]
        if not model_s3_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {model_s3_uri}")
        bucket, key = model_s3_uri[5:].split("/", 1)

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        local_dir = os.path.join(LOCAL_MODEL_DIR, ds_nodash)
        os.makedirs(local_dir, exist_ok = True)

        tar_path = os.path.join(local_dir, "model.tar.gz")
        s3.get_conn().download_file(bucket, key, tar_path)

        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(local_dir)

        return local_dir

    ok = verify_raw_files()
    processed = preprocess_to_pt(ok)
    model_uri = start_training(processed)
    download_and_extract_model(model_uri)

dag = mnist_sagemaker_lenet_local()



