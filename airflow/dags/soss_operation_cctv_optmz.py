from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pendulum import timezone

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

# UTC -> KST 타임존 변경
def convert_utc_to_kst(ts):
    dt = datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%S%z") + relativedelta(hours=9)
    return dt.strftime("%Y%m%d")

with DAG(
    dag_id="soss_operation_cctv_optmz",
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

    gu_list = [
        "gyeryong_si",
        "cheongyang_gun",
        "seocheon_gun",
        "geumsan_gun",
        "buyeo_gun",
        "hongseong_gun",
        "cheonan_si_seobuk_gu",
        "yesan_gun",
        "taean_gun",
        "cheonan_si_dongnam_gu",
        "boryeong_si",
        "nonsan_si",
        "gongju_si",
        "seosan_si",
        "asan_si",
        "dangjin_si"
    ]

    tasks = []

    for gu_nm in gu_list:

        with TaskGroup(group_id=f"{gu_nm}") as gu_task:
            # CCTV 최적화 운영
            cctv_optmz_operation = BashOperator(
                task_id=f"cctv_optmz_operation_{gu_nm}",
                bash_command="python /root/soss/soss cctv-optmz-operation {{ data_interval_end | convert_utc_to_kst }} " + gu_nm,
                trigger_rule="all_done"
            )   

            cctv_optmz_operation

        tasks.append(gu_task)

    # 구별 CCTV 모니터시간
    dm_gu_cctv_mntr_tm = BashOperator(
        task_id="dm_gu_cctv_mntr_tm",
        bash_command="python /root/soss/soss spark_insert_dm_gu_cctv_mntr_tm {{ data_interval_end | convert_utc_to_kst }} NONE"
    )

    # 구별 CCTV 모니터비율
    dm_gu_cctv_mntr_rate = BashOperator(
        task_id="dm_gu_cctv_mntr_rate",
        bash_command="python /root/soss/soss spark_insert_dm_gu_cctv_mntr_rate {{ data_interval_end | convert_utc_to_kst }} NONE"
    )
    
    # DAG dependencies
    for i in range(len(tasks)-1):
        tasks[i] >> tasks[i+1]
    else:
        tasks[-1] >> [ dm_gu_cctv_mntr_tm, dm_gu_cctv_mntr_rate ]