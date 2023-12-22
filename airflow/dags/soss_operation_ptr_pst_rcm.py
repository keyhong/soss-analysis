from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pendulum import timezone

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# UTC -> KST 타임존 변경
def convert_utc_to_kst(ts):
    dt = datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%S%z") + relativedelta(hours=9)
    return dt.strftime("%Y%m%d")

with DAG(
    dag_id="soss_operation_ptr_pst_rcm",
    start_date=datetime(2023, 6, 1, tzinfo=timezone("Asia/Seoul")),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    user_defined_filters={"convert_utc_to_kst": convert_utc_to_kst}
) as dag:

    # 순찰거점 운영 
    ptr_pst_rcm_operation = BashOperator(
        task_id="ptr_pst_rcm_operation",
        bash_command="python /root/soss/soss ptr-pst-rcm-operation {{ data_interval_end | convert_utc_to_kst }} NONE"
    )

    # Trigger
    trigger_soss_operation_cctv_optmz = TriggerDagRunOperator(
        task_id="trigger_soss_operation_cctv_optmz",
        trigger_dag_id="soss_operation_cctv_optmz",
        execution_date="{{ data_interval_end }}",
        reset_dag_run=True,
        trigger_rule="all_done"
    )

    ptr_pst_rcm_operation >> trigger_soss_operation_cctv_optmz