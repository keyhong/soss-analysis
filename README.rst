.. -*- mode: rst -*-

What is SOSS?
---------------
**SOSS** 은 지역 안전 예측 프로그램 입니다.


SOSS Analysis Program
-----------------------

* safe-idex : 안전지수
  
  - safe-idex-preprocessing : 안전지수 전처리

  - safe-idex-model-learning : 안전지수 모델 학습

  - safe-idex-operation : 안전지수 운영

* cctv-optmz : CCTV 최적화

  - cctv-optmz-model-learning : CCTV 최적화 모델 학습
  
  - cctv-optmz-operation : CCTV 최적화 운영

* ptr-pst-rcm : 순찰거점

  - ptr-pst-rcm : 순찰거점 운영

* statistic : 통계 데이터 적재

  - spark_insert_dm_npa_dcr_rcp : 경찰청 신고 통계 적재 

  - spark_insert_dm_gu_cctv_mntr_tm : CCTV 구별 시간 통계 적재

  - spark_insert_dm_gu_cctv_mntr_rate : CCTV 구별 비율 통계 적재

Installation
------------

.. code-block:: bash
    
  cd /home/icitydatahub/soss && pip install -e . 

Execution
-----------
| 프로그램을 실행시키기 위한 일별, 월별 프로그램을 실행시킬수 있는 airflow 스케줄링 파일 ($AIRFLOW_HOME/dags)이 만들어져 있습니다.
| 따라서 스케줄링을 실행하고 airflow-webserver 에서 실행 확인 필요
| airflow-webserver : http://172.19.100.217:8080/home

Dependencies
------------

soss requires python :

- Python (>= 3.6)

**soss는 Python 3.6 버전 이상의 파이썬에서 동작하며, 현재 인천시 기준 설치버전은 Python 3.8.17 입니다.**

soss requires packages:

- apache-airflow[postgres]==2.6.3
- pyspark==3.3.2
- py4j==0.10.9.5
- numpy==1.24.3
- pandas==1.4.1
- geopandas==0.13.2
- scikit-learn==1.2.2
- xgboost==1.6.2
- lightgbm==3.3.5
- joblib==1.2.0
- python-dateutil
- tdqm
- haversine

**soss는 스케줄링 프로그램으로 airflow를 사용하고 있으며, airflow의 버전은 2.6.2 입니다.**

**soss는 분산 처리 프레임워크는 pyspark를 사용하고 있으며, pyspark의 버전은 3.3.2 입니다.**
