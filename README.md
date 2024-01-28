SOSS
======

# What is SOSS?

**SOSS** 은 지역 안전 예측 프로그램 입니다.

안전지수 분석, CCTV 효율지수 분석, 순찰거점 추천, [CCTV 효율지수, 신고접수] 통계 분석 서비스를 제공합니다.

# SOSS Analysis Program

* safe-idex : 안전지수 분석
  
  - safe-idex-preprocessing : 안전지수 전처리

  - safe-idex-model-learning : 안전지수 모델 학습

  - safe-idex-operation : 안전지수 운영

* cctv-optmz : CCTV 효율지수 분석

  - cctv-optmz-model-learning : CCTV 최적화 모델 학습
  
  - cctv-optmz-operation : CCTV 최적화 운영

* ptr-pst-rcm : 순찰거점 추천

  - ptr-pst-rcm : 순찰거점 운영

* statistic : [CCTV 효율지수, 경찰신고접수] 통계 분석

  - spark_insert_dm_npa_dcr_rcp : 경찰 신고 통계 적재 

  - spark_insert_dm_gu_cctv_mntr_tm : CCTV 구별 시간 통계 적재

  - spark_insert_dm_gu_cctv_mntr_rate : CCTV 구별 비율 통계 적재

# Installation

.. code-block:: bash
    
  $ cd /home/icitydatahub/soss && pip install -e . 

# Execution

| 1. 프로그램을 실행하기 위해서는 Postgres Data Warehouse에 전처리된 데이터들이 적재되어 있어야 합니다.
|
| 2. SOSS 패키지를 빌드 후, 모든 서비스의 실행은 __main__.py 모듈의 단일 EntryPoint를 통해 실행됩니다.
|

```bash
$ usage: python soss [-h] [SERVICE] [EXECUTION_DATE]

$ example: python soss safe-idex-operation 20230101
```
| 실행에 대한 SERVICE 목록에 대한 참조는 help를 통해 확인 가능합니다.
| ** EXECUTION_DATE 인자 값에 대한 포맷은 'yyyyMMd' 이며, 일치하지 않을 시 에러를 반환하게 되어있습니다.


```bash
$ usage: python soss -h
```

| 3. 미리 작성된 월별, 일별 DAG가 있어 bin/start-soss-airflow-web-scheduler.sh 쉘을 실행하면 배치 프로그램이 매일 수행됩니다. 
| 프로그램 수행에 대한 결과는 Airflow Webserver 에서 확인 가능합니다. *Airflow Webserver : http://127.0.0.1:8080/home*

# Dependencies

soss requires python :

- Python (>= 3.6)

**soss는 f-string 사용으로 인해 Python 3.6 버전 이상의 파이썬에서 동작하며, 현재 테스트한 파이썬 설치버전은 Python 3.8.17 입니다.**

soss requires packages:

| Feature                                   | Details                                                                                                                                            | Version  |
| ------------------------------------------| ---------------------------------------------------------------------------------------------------------------------------------------------------|----------| 
| apache-airflow[postgres] | an open-source platform for orchestrating complex workflows and data pipelines.                                                                                     | 2.6.3    |
| pyspark                  | Python library for Apache Spark, an open-source distributed computing system.                                                                                       | 3.3.2    |
| py4j                     | It enables Python programs running in a Python interpreter to dynamically access Java objects in a Java virtual machine (JVM).                                      | 0.10.9.5 |
| numpy                    | a fundamental package for scientific computing in Python and is widely used in various fields such as machine learning, data analysis, signal processing, and more. | 1.24.3   |
| pandas                   | an open-source data manipulation and analysis library for Python.                                                                                                   | 1.4.1    |
| geopandas                | It extends the capabilities of the Pandas library to handle geospatial data.                                                                                        | 0.13.2   |
| scikit-learn             | It provides simple and efficient tools for data analysis and modeling.                                                                                              | 1.2.2    |
| xgboost                  | It provides an efficient and scalable implementation of the gradient boosting framework.                                                                            | 1.6.2    |
| lightgbm                 | distributed, high-performance gradient boosting framework that is designed for efficient training and prediction of large-scale datasets.                           | 3.3.5    |
| joblib                   | It provides tools for performing parallel computing and efficiently distributing computational tasks across multiple processors or even different machines.         | 1.2.0    |
| python-dateutil          | It provides extensions to the standard datetime module.                                                                                                             |          |
| tdqm                     | It provides a simple and customizable way to display progress bars for tasks that may take some time to complete, making it easier for users to track the progress of an operation. | |
| haversine                | It provides a simple interface for calculating distances between two points on the Earth using the haversine formula.                                                               | |


- soss는 스케줄링 프로그램은은 Airflow를 사용하고 있으며, 테스트 버전은 2.6.2 입니다.*
- soss는 분산 처리 프레임워크는 PySpark를 사용하고 있으며, 테스트 버전은 3.3.2 입니다.*
