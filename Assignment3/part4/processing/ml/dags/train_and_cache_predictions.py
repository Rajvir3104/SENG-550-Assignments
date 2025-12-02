import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # <-- FIXED IMPORT


def run_train_script():
    base = os.environ.get("PROJECT_BASE_DIR")
    if not base:
        raise Exception("PROJECT_BASE_DIR not set")

    script = os.path.join(base, "train.py")
    print(f"Running training script: {script}")

    env = os.environ.copy()
    env["PROJECT_BASE_DIR"] = base
    env.setdefault("REDIS_CACHE_HOST", "localhost")
    env.setdefault("REDIS_CACHE_PORT", "6379")

    result = subprocess.run(
        ["python3", script],
        capture_output=True,
        text=True,
        env=env,
    )

    print("=== TRAIN STDOUT ===")
    print(result.stdout)
    if result.returncode != 0:
        print("=== TRAIN STDERR ===")
        print(result.stderr)
        raise Exception("Training failed")


def run_cache_script():
    base = os.environ.get("PROJECT_BASE_DIR")
    if not base:
        raise Exception("PROJECT_BASE_DIR not set")

    script = os.path.join(base, "cache_predictions.py")
    print(f"Running cache predictions script: {script}")

    env = os.environ.copy()
    env["PROJECT_BASE_DIR"] = base
    env.setdefault("REDIS_CACHE_HOST", "localhost")
    env.setdefault("REDIS_CACHE_PORT", "6379")

    result = subprocess.run(
        ["python3", script],
        capture_output=True,
        text=True,
        env=env,
    )

    print("=== CACHE STDOUT ===")
    print(result.stdout)
    if result.returncode != 0:
        print("=== CACHE STDERR ===")
        print(result.stderr)
        raise Exception("Caching predictions failed")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="train_and_cache_predictions",
    default_args=default_args,
    description="Train model and cache predictions in Redis",
    schedule_interval=timedelta(seconds=20),  # every 20 seconds
    catchup=False,
) as dag:

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=run_train_script,
    )

    cache_task = PythonOperator(
        task_id="cache_predictions",
        python_callable=run_cache_script,
    )

    train_task >> cache_task
