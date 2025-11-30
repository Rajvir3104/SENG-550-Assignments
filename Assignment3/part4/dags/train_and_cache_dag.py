from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

def train_model():
    """Execute the Spark training script"""
    script_path = '/project/processeing/ml/train.py'
    base_dir = '/project'
    
    print(f"Running training script: {script_path}")
    
    env = os.environ.copy()
    env['PROJECT_BASE_DIR'] = base_dir
    
    result = subprocess.run(
        ['python3', script_path],
        capture_output=True,
        text=True,
        env=env
    )
    
    print("=== STDOUT ===")
    print(result.stdout)
    
    if result.returncode != 0:
        print("=== STDERR ===")
        print(result.stderr)
        raise Exception(f"Training failed with return code {result.returncode}")
    
    print("Training completed successfully!")

def cache_predictions():
    """Execute the cache predictions script"""
    script_path = '/project/processeing/ml/cache_predictions.py'
    base_dir = '/project'
    
    print(f"Running cache predictions script: {script_path}")
    
    env = os.environ.copy()
    env['PROJECT_BASE_DIR'] = base_dir
    env['REDIS_CACHE_HOST'] = os.getenv('REDIS_CACHE_HOST', 'redis-cache')
    env['REDIS_CACHE_PORT'] = os.getenv('REDIS_CACHE_PORT', '6379')
    
    result = subprocess.run(
        ['python3', script_path],
        capture_output=True,
        text=True,
        env=env
    )
    
    print("=== STDOUT ===")
    print(result.stdout)
    
    if result.returncode != 0:
        print("=== STDERR ===")
        print(result.stderr)
        raise Exception(f"Cache predictions failed with return code {result.returncode}")
    
    print("Cache predictions completed successfully!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='train_and_cache_predictions',
    default_args=default_args,
    description='Train model and cache predictions in Redis every 20 seconds',
    schedule=timedelta(seconds=20),
    catchup=False,
    tags=['machine-learning', 'spark', 'redis', 'pipeline'],
) as dag:

    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )
    
    cache_task = PythonOperator(
        task_id='cache_predictions',
        python_callable=cache_predictions,
    )