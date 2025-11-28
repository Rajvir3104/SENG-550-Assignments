from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Project dir where part2 lives
PROJECT_DIR = "/Users/rajvir/Desktop/School/Year3/Year4/SENG-550-Assignments/Assignment3/part2"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="incremental_orders_aggregation",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(seconds=4),  # every 4 seconds
    catchup=False,
) as dag:

    run_incremental_spark = BashOperator(
        task_id="run_incremental_spark",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            # venv is three levels up from part2/incremental
            "source ../../venv/bin/activate && "
            "python incremental/incremental_spark.py"
        ),
    )
