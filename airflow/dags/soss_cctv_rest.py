from datetime import timedelta, datetime
from pendulum import timezone

from airflow.models import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="soss_cctv_rest_api",
    start_date=datetime(2023, 8, 1, tzinfo=timezone("Asia/Seoul")),
    schedule_interval="0 1 15 * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10)
    }
) as dag:

        cctv_rest_api_operation = BashOperator(
            task_id=f"cctv_rest_api_operation",
            bash_command="python /root/cctv-rest/cctv_rest.py"
        )