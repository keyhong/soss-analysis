from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pendulum import timezone

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

# UTC -> KST 타임존 변경
def convert_utc_to_kst(ts):
    dt = datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%S%z") + relativedelta(hours=9)
    return dt.strftime("%Y%m%d")
        
with DAG(
    dag_id="soss_model_learning_safe_idex",
    start_date=datetime(2023, 8, 1, tzinfo=timezone("Asia/Seoul")),
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

    ssh_port = 7465
    passwd = "Ehcjd4312!"

    for gu_nm in gu_list:

        with TaskGroup(group_id=f"{gu_nm}") as gu_task:
            # 안전지수 학습
            safe_idex_model_learning = BashOperator(
                task_id=f"safe_idex_model_learning_{gu_nm}",
                bash_command="python /root/soss/soss safe-idex-model-learning {{ data_interval_end | convert_utc_to_kst }} " + gu_nm,
                trigger_rule="all_done"
            )   
            
            """
            # 안전지수 학습 모델(위해지표) 전송
            model_hazard_transfer = BashOperator(
                task_id=f"model_hazard_{gu_nm}_transfer",
                bash_command=f"sshpass -p '{passwd}' scp -P {ssh_port} -r /root/soss/soss/safe_idex/model_learning/model/hazard/{gu_nm} root@172.31.20.155:/root/soss/soss/safe_idex/model_learning/model/hazard",
                execution_timeout=timedelta(minutes=3)
            )
            
            # 안전지수 학습 모델(취약지표) 전송
            model_weak_transfer = BashOperator(
                task_id=f"model_weak_{gu_nm}_transfer",
                bash_command=f"sshpass -p '{passwd}' scp -P {ssh_port} -r /root/soss/soss/safe_idex/model_learning/model/weak/{gu_nm} root@172.31.20.155:/root/soss/soss/safe_idex/model_learning/model/weak",
                execution_timeout=timedelta(minutes=3)
            )
            """
            
            # safe_idex_model_learning >> model_hazard_transfer >> model_weak_transfer
            safe_idex_model_learning

        tasks.append(gu_task)

        
    # Trigger
    trigger_soss_model_learning_cctv_optmz = TriggerDagRunOperator(
        task_id="trigger_soss_model_learning_cctv_optmz",
        trigger_dag_id="soss_model_learning_cctv_optmz",
        execution_date="{{ data_interval_end }}",
        reset_dag_run=True,
        trigger_rule="all_done"
    )

    for i in range(len(tasks)-1):
        tasks[i] >> tasks[i+1]
    else:
        tasks[-1] >> trigger_soss_model_learning_cctv_optmz
