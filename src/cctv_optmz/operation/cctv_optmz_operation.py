#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CCTV 최적화 운영 모듈
"""

from __future__ import annotations

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import os
from typing import (
    List,
    Dict,
)
import time

from dateutil.relativedelta import relativedelta
import joblib
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.utils import ParseException
from py4j.protocol import Py4JJavaError
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from soss.queries.cctv_optmz_query import CctvOptmzQuery
from soss.spark_session.db_connector import SparkClass
from soss.utils.config_parser import *

MODEL_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "model_learning", "model")
SCALER_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "model_learning", "scaler")
MONTH_PERIOD = 3
HALF_MONTH_PERIOD = MONTH_PERIOD // 2

def delete_soss_cctv_eff_idex(today: str, gu_nm: str, gu_cd: int):

    # define delete date
    delete_date = datetime.strptime(today, "%Y%m%d")
    delete_date = (delete_date - relativedelta(days=2))
    delete_date = datetime.strftime(delete_date, "%Y%m%d")
    
    # Create an engine to connect to the database
    engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWRD}@{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}")

    # 세션을 사용하여 데이터베이스 작업 수행
    with Session(engine) as session:

        # CCTV 효율지수 - 그리드, 행정동, 시군구 테이블
        tables = (
            "SOSS.DM_CCTV_EFF_IDEX_GRID",
            "SOSS.DM_CCTV_EFF_IDEX_ADMD",
            "SOSS.DM_CCTV_EFF_IDEX_SGG"
        )

        try:
            for table in tables:
                # 데이터 삭제
                session.execute(f"DELETE FROM {table} WHERE stdr_de <= '{delete_date}' AND gu_cd='{gu_cd}'")
                time.sleep(10)
                
                session.execute(f"DELETE FROM {table} WHERE stdr_de = '{today}' AND gu_cd='{gu_cd}'")
                time.sleep(10)

            # 정상 수행시 commit
            session.commit()
            logging.info(f"DatMart {tables[0]} / {tables[1]} / {tables[2]} {gu_nm} {delete_date} 이전 일자 Delete!")
            logging.info(f"DatMart {tables[0]} / {tables[1]} / {tables[2]} {gu_nm} {today} 일자 Delete!")
        except:
            # 예외가 발생한 경우 롤백
            session.rollback()
            logging.error(f"Error raisd. Transaction Rollback!")

    engine.dispose()

def create_unsafe_idex() -> pd.DataFrame:
    """01. 불안전지수 데이터를 생성하는 함수

    Returns
    -------
    unsafe_df : pandas.DataFrame
       오늘 날짜의 불안전지수 데이터
    """

    # (1) 안전지수 데이터 가져오기
    try:
        safe_df = query_obj.get_safe_idex_sql()

        if not safe_df.take(1):
            raise Exception("Fucntion : get_safe_idex_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        safe_df = safe_df.toPandas()

    # (100 - 안전지수)로 불안전지수 산출
    unsafe_df = safe_df.copy()
    unsafe_df["unsafe_idex"] = 100 - unsafe_df["safe_idex"]

    # (2) 불안전지수 점수 스케일링 : 0 ~ 70 점수 표현
    min_max_scaler = MinMaxScaler(feature_range=(0, 70))
    unsafe_df["unsafe_idex"] = min_max_scaler.fit_transform(unsafe_df[["unsafe_idex"]])

    # (3) 소수점 둘째 자리까지만 표현
    unsafe_df["unsafe_idex"] = round(unsafe_df["unsafe_idex"], 2)

    # (4) 불필요 컬럼 제거
    unsafe_df.drop("safe_idex", axis=1, inplace=True)

    return unsafe_df

def merge_land(unsafe_df: pd.DataFrame) -> pd.DataFrame:
    """02. 환경요인(토지비율) 데이터를 결합하는 함수

    Parametersrh
    ----------
    unsafe_df : pandas.DataFrame
       데이터를 결합할 기준 테이블

    Returns
    -------
    land_merge_df : pandas.DataFrame
       안전지수, 토지비율 데이터를 결합한 데이터 프레임
    """

    # (1) 토지특성 데이터 가져오기
    try:
        land_df = query_obj.get_land_per_sql()

        if not land_df.take(1):
            raise Exception("Fucntion : get_land_per_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        land_df = land_df.toPandas()

    # 주거지, 상업지, 공업지인 토지 정보
    land_df = land_df[land_df["land_cat"] != "cd4"]

    # pivot으로 행정동별 토지 비율을 값을 컬럼으로 변환
    land_df = land_df.pivot_table(index="admd_cd", columns="land_cat", values="land_per", aggfunc="mean", fill_value=0)
    land_df.reset_index(inplace=True)

    # pivot_table에서 특정 토지이용 분류 데이터가 없다면 생성
    cols = { "cd1", "cd2", "cd3" }
    land_df[list(cols.difference(land_df.columns))] = 0

    # (2) 요인분석 가중치 적용
    weight = np.array([1.4, 1.35, 1.25]).reshape(3, 1) # 사전 요인 분석을 통해 계산된 가중치
    land_df["land_per"] = np.dot(land_df[["cd1", "cd2", "cd3"]].values, weight)

    # (3) 토지비율 점수 스케일링 : 0 ~ 10
    min_max_scaler = MinMaxScaler(feature_range=(0, 10))
    land_df["land_per"] = min_max_scaler.fit_transform(land_df[["land_per"]])

    # (4) 소수점 둘째 자리까지만 표현
    land_df["land_per"] = round(land_df["land_per"], 2)

    # (5) 데이터 프레임 결합하기
    land_merge_df = unsafe_df.merge(land_df[["admd_cd", "land_per"]], how="left", on="admd_cd")

    land_merge_df["land_per"].fillna(0, inplace=True)
    # 결측값 원인 차후 확인

    return land_merge_df

def merge_report(land_merge_df: pd.DataFrame) -> pd.DataFrame:
    """03. 범죄요인(5대 범죄 신고건수) 데이터를 가져오는 함수

    Parameters
    ----------
    merge_df : pandas.DataFrame
       데이터를 결합할 기준 테이블

    Returns
    -------
    report_merge_df : pandas.DataFrame
       안전지수, 토지비율, 신고건수 데이터를 결합한 데이터 프레임
    """

    # C0, C1 긴급사건 신고접수 데이터 불러오기
    try:
        # 일자 설정
        start_month = (today_dt - relativedelta(months=1)).strftime("%m")
        end_month = today_dt.strftime("%m")

        #(1개월 2주전 ~ 어제) 경찰청신고접수 데이터 불러오는 코드
        report_df = query_obj.get_npa_dcr_rcp_sql(start_month, end_month)

        if not report_df.take(1):
            raise Exception("Fucntion : get_npa_stt_rcp_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        report_df = report_df.toPandas()

    # 행정동별-시간별 5대신고 수, 총 신고 수
    report_df = report_df.pivot_table(index=["admd_cd", "stdr_tm"], values=["import_report_cnt", "all_report_cnt"])
    report_df.reset_index(inplace=True)

    # 행정동별-시간별로 총 신고수 중에서 긴급신고 수의 비율을 계산
    report_df["report_per"] = report_df["import_report_cnt"] / report_df["all_report_cnt"]

    # (2) 5대범죄신고건수 점수 스케일링 : 0 ~ 10
    min_max_scaler = MinMaxScaler(feature_range=(0, 10))
    report_df["report_per"] = min_max_scaler.fit_transform(report_df[["report_per"]])

    # (3) 소수점 둘째 자리까지만 표현
    report_df["report_per"] = round(report_df["report_per"], 2)

    # (4) 데이터 프레임 결합하기
    report_merge_df = land_merge_df.merge(report_df[["stdr_tm", "admd_cd", "report_per"]], how="left", on=["stdr_tm", "admd_cd"])
    report_merge_df["report_per"].fillna(0, inplace=True)

    return report_merge_df

def merge_women_pop(report_merge_df: pd.DataFrame) -> pd.DataFrame:
    """04. 유동인구요인 (범죄취약계층 여성 유동인구) 데이터를 결합하는 함수

    Parameters
    ----------
    report_merge_df : pandas.DataFrame
       예측할 오늘날의 데이터

    Returns
    -------
    women_pop_merge_df : pandas.DataFrame
       안전지수, 토지비율, 신고건수, 유동인구 데이터를 결합한 데이터 프레임
    """

    def create_predict_df(report_merge_df: pd.DataFrame) -> pd.DataFrame:
        """04-(1) 예측용 데이터를 생성하는 함수

        Parameters
        ----------
        report_merge_df : pandas.DataFrame
           통합 데이터 마트 데이터프레임

        Returns
        -------
        merge_df : pandas.DataFrame
           예측용 데이터
        """

        # 04-(1) 통계용 데이터 로딩
        try:
            for year in range(0, 5):

                # 일자 설정
                start_date = (today_dt - relativedelta(years=year, months=HALF_MONTH_PERIOD, weeks=2) + relativedelta(days=1)).strftime("%Y%m%d")
                end_date = (today_dt - relativedelta(years=year, days=1)).strftime("%Y%m%d")

                # (1개월 2주 전 ~ 어제) 통계 데이터를 불러오는 코드
                statistic_df = query_obj.get_statistic_women_pop_sql(start_date, end_date)

                if statistic_df.take(1):
                    break
        except (ParseException, Py4JJavaError) as e:
            logging.error(str(e))

        # 통계용 데이터 요일 컬럼 생성
        statistic_df = statistic_df.withColumn("weekday", F.to_date(F.unix_timestamp("stdr_de", "yyyyMMdd").cast("timestamp")))
        statistic_df = statistic_df.withColumn("weekday", F.date_format("weekday", "E"))

        # 통계용 데이터 groupby
        statistic_df = statistic_df.groupby(["grid_id", "stdr_tm", "weekday"]).agg(F.mean("women_pop").alias("women_pop_mean"), F.max("women_pop").alias("women_pop_max"))
        statistic_df = statistic_df.toPandas()

        #  04-(2) 데이터 마트 생성 (학습용 데이터와 통계용 데이터 결합)
        week_dict = { 0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun" }
        report_merge_df["weekday"] = datetime.today().weekday()
        report_merge_df["weekday"] = report_merge_df["weekday"].map(lambda x: week_dict[x])

        statistic_merge_df = report_merge_df.merge(statistic_df, how="left", on=["grid_id", "stdr_tm", "weekday"])
        statistic_merge_df.fillna(value={"women_pop_mean": 0, "women_pop_max": 0}, inplace=True)

        # 04-(3) 버스정류소 데이터 결합
        try:
            bus_stop_cnt_df = query_obj.get_bus_stop_cnt_sql()
            if not bus_stop_cnt_df.take(1):
                raise Exception("Fucntion : get_bus_stop_cnt_sql() 쿼리로 가져오는 데이터가 없습니다")
        except (ParseException, Py4JJavaError) as e:
            logging.error(str(e))
        else:
            bus_stop_cnt_df = bus_stop_cnt_df.toPandas()

        # 학습용데이터에 버스정류소 개수 데이터 결합
        bus_merge_df = statistic_merge_df.merge(bus_stop_cnt_df, how="left", on="grid_id")
        bus_merge_df["bus_stop_cnt"].fillna(0, inplace=True)

        return bus_merge_df

    def predict_women_pop(predict_df: pd.DataFrame) -> pd.DataFrame:
        """04-(2) 예측용 데이터를 예측하는 함수

        Parameters
        ----------
        predict_df : pandas.DataFrame
           예측용 데이터

        Returns
        -------
        merge_df : pandas.DataFrame
           예측 결과 데이터
        """

        predict_X = predict_df[["women_pop_mean", "women_pop_max", "bus_stop_cnt", "weekday"]]

        # Load MinMaxScaler
        min_max_scaler = joblib.load(os.path.join(SCALER_FOLDER, f"{gu_nm}_scaler.pkl"))

        # 연속형 변수(여성 유동인구수 평균, 여성 유동인구수 최대값, 버스 정류소 개수) Scaling
        predict_X_continuous = predict_X[["women_pop_mean", "women_pop_max", "bus_stop_cnt"]]

        # MinMaxScaler Transform
        predict_X_continuous =  min_max_scaler.transform(predict_X_continuous)

        # 범주형 변수(요일) One-Hot Encoding
        predict_X_categorical = pd.get_dummies(predict_X[["weekday"]], columns=["weekday"])

        # 없는 요일 범주형 변수 만들기
        days = [ "weekday_Mon", "weekday_Tue", "weekday_Wed", "weekday_Thu", "weekday_Fri", "weekday_Sat", "weekday_Sun" ]

        for day in days:
            if day not in predict_X_categorical.columns:
                predict_X_categorical[day] = np.uint8(0)
        else:
            predict_X_categorical = predict_X_categorical[days]
            predict_X_categorical = np.array(predict_X_categorical)

        # 분리된 예측 변수들을 하나의 array로 합치기
        predict_X = np.concatenate((predict_X_continuous, predict_X_categorical), axis=1)

        cols = [
            "women_pop_mean", "women_pop_max", "bus_stop_cnt",
            "weekday_Mon", "weekday_Tue", "weekday_Wed", "weekday_Thu", "weekday_Fri", "weekday_Sat", "weekday_Sun"
        ]

        predict_X = pd.DataFrame(predict_X, columns=cols)

        # Load RandomForest Regression Model
        rf = joblib.load(os.path.join(MODEL_FOLDER, f"{gu_nm}_rf.pkl"))

        # 모델 예측 값 기록
        predict_df["women_pop"] = rf.predict(predict_X)

        predict_df.drop(["women_pop_mean", "women_pop_max", "bus_stop_cnt", "weekday"], axis=1, inplace=True)

        return predict_df

    # 01-(1) 예측할 데이터 생성하기
    predict_df = create_predict_df(report_merge_df)

    # 01-(2) 데이터 예측하기
    women_pop_merge_df = predict_women_pop(predict_df)

    # 01-(3) 유동인구요인 점수 스케일링 : 0 ~ 10
    min_max_scaler = MinMaxScaler(feature_range=(0, 10))
    women_pop_merge_df["women_pop"] = min_max_scaler.fit_transform(women_pop_merge_df[["women_pop"]])

    # 소수점 둘째 자리까지만 표현
    women_pop_merge_df["women_pop"] = round(women_pop_merge_df["women_pop"], 2)

    return women_pop_merge_df

def merge_cctv(women_pop_merge_df: pd.DataFrame) -> pd.DataFrame:
    """05. 감시지수 데이터를 가져오는 함수

    Parameters
    ----------
    women_pop_merge_df : pandas.DataFrame

    Returns
    -------
    cctv_merge_df : pandas.DataFrame
       안전지수, 토지비율, 신고건수, 유동인구, CCTV점수 데이터를 결합한 데이터 프레임
    """

    def print_grid_id(x_id: int, y_id: int, distance) -> List[str]:
        """공간격자를 distance 수만큼 돌면서 주변 격자값을 늘려주는 함수

        Parameters
        ----------
        x_id : int
           공간 확장할 그리드의 X좌표
        y_id : int
           공간 확장할 그리드의 Y좌표
        distance : int
           늘릴 격자의 개수

        Return
        -------
        grid_list : list

        """
        x_list = [ x_id + i for i in range(-distance, distance+1) ]
        x_list = list(map(str, x_list))

        y_list = [ y_id + i for i in range(-distance, distance+1) ]
        y_list = list(map(str, y_list))

        grid_list = []

        for x in x_list:
            for y in y_list:
                grid_list.append(",".join([x, y]))

        return grid_list


    # (1) CCTV 데이터 로딩
    try:
        cctv_df = query_obj.get_cctv_cnt_sql()
        if not cctv_df.take(1):
            raise Exception("Fucntion : get_cctv_cnt_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        cctv_df = cctv_df.toPandas()

        cctv_df["cctv_score"] = 0

    # (2) CCTV_CO 점수 구하기 (거리에 따른 가중치 심화(거리제곱)
    # distance 1 : 50m씩 범위 확대
    distance = 2

    # 점수 감소분 가중치
    weight = 0.1

    # 공간 확장할 데이터(CCTV 개수가 1개 이상)들의 데이터
    source_df = cctv_df[cctv_df["cctv_co"] > 0].copy()

    for idx, row in source_df.iterrows():

        # 공간 확장할 데이터의 row_no
        x_id = int(row["row_no"])

        # 공간 확장할 데이터의 clm_no
        y_id = int(row["clm_no"])

        # 공간 확잘할 데이터의 cctv 개수
        cctv_co = row["cctv_co"]

        # 거리만큼 격자값 늘리기
        for distance in range(0, distance+1):

            # 늘린 공간격자들의 행렬번호
            expand_mtr_no = print_grid_id(x_id, y_id, distance)

            # 늘린 공간격자들의 행렬번호가 전체 그리드에 있는 지 유의미성 판단
            df_meaningful = cctv_df[cctv_df["mtr_no"].isin(expand_mtr_no)]

            # 늘어난 격자의 CCTV 가중치 더한 값 추가하기
            for idx, _ in df_meaningful.iterrows():

                # CCTV 개수 * ( ( 1- 거리의 제곱 ) * 점수 감소분 가중치 )
                weight_value = cctv_co * ( 1.0 - ( (distance+1)**2 * weight) ) # 거리에 따라 가중치 곱하기

                original_val = cctv_df.at[idx, "cctv_score"] # 더하기 전 cctv 가중치 값
                cctv_df.at[idx, "cctv_score"] = (original_val + weight_value)

    # (3) 감시지수 점수 스케일링 : 0 ~ 100
    min_max_scaler = MinMaxScaler(feature_range=(0, 10))
    cctv_df["cctv_score"] = min_max_scaler.fit_transform(cctv_df[["cctv_score"]])

    # 소수점 둘째 자리까지만 표현
    cctv_df["cctv_score"] = round(cctv_df["cctv_score"], 2)

    # (4) CCTV테이블 병합
    cctv_merge_df = women_pop_merge_df.merge(cctv_df[["grid_id", "cctv_score"]], how="left", on="grid_id")
    cctv_merge_df["cctv_score"].fillna(0, inplace=True)

    return cctv_merge_df

def save_cctv_eff_idex(cctv_merge_df: pd.DataFrame) -> None:
    """06. CCTV 효율지수를 저장하는 함수

    Parameters
    ----------
    cctv_merge_df : pandas.DataFrame
       안전지수, 토지비율, 신고건수, 유동인구, CCTV점수 데이터를 결합한 데이터 프레임
    """

    # (1) 범죄취약지수 산출 = 불안전지수 + 토지비율 점수 + 유동인구 점수 + 신고건수 점수
    cctv_merge_df["weak_score"] = cctv_merge_df["unsafe_idex"] + cctv_merge_df["land_per"] + cctv_merge_df["women_pop"] + cctv_merge_df["report_per"]
    cctv_merge_df.drop(["unsafe_idex", "land_per", "women_pop", "report_per"], axis=1, inplace=True)

    # (2) 감시지수 산출

    # 유동인구 합계 데이터 가져오기
    try:
        for year in range(0, 5):

            start_date = (today_dt - relativedelta(years=year, months=MONTH_PERIOD)).strftime("%Y%m%d")
            end_date = (today_dt - relativedelta(years=year, days=1)).strftime("%Y%m%d")

            pop_sum_df = query_obj.get_pop_sum_sql(start_date, end_date)

            if pop_sum_df.take(1):
                break
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        pop_sum_df = pop_sum_df.toPandas()

    # 유동인구 합계 데이터 병합
    pop_merge_df = cctv_merge_df.merge(pop_sum_df, how="left", on=["grid_id", "stdr_tm"])
    pop_merge_df["fpop_sum"].fillna(0, inplace=True)

    # 한달간 그 시간대 유동인구수가 0 이면(사람이 없다면) 감시지수 10(감시력이 떨어지는 경우)으로 처리
    pop_merge_df["cctv_score"] = np.where(pop_merge_df["fpop_sum"] == 0, 10, pop_merge_df["cctv_score"])

    # CCTV 효율지수 산출
    pop_merge_df["cctv_eff_idex"] = pop_merge_df["weak_score"] - pop_merge_df["cctv_score"]
    pop_merge_df["cctv_eff_idex"] = np.where(pop_merge_df["cctv_eff_idex"] < 0, 0, pop_merge_df["cctv_eff_idex"])
    pop_merge_df["cctv_eff_idex"] = 100 - pop_merge_df["cctv_eff_idex"]

    # 일자와 구 코드 기입
    pop_merge_df["stdr_de"] = today
    pop_merge_df["gu_cd"] = gu_cd
    logging.info(f"pop_merge_df Count : {pop_merge_df.shape[0]:,}")
    logging.info(f"pop_merge_df Null Check\n{pop_merge_df.isnull().sum()}\n")
    
    # 시간 두자리 수로 채우기
    pop_merge_df["stdr_tm"] = pop_merge_df["stdr_tm"].apply(lambda x: str(x).zfill(2))

    # 최종 컬럼 정리
    cctv_eff_idex_grid_df = pop_merge_df[["stdr_de", "stdr_tm", "grid_id", "gu_cd", "admd_cd", "cctv_eff_idex"]].copy()

    # CCTV효율지수가 40미만인 경우, 필요 CCTV개수를 1개 부여
    cctv_eff_idex_grid_df["need_cctv_co"] = np.where(cctv_eff_idex_grid_df["cctv_eff_idex"] <= 40, 1, 0)

    # CCTV 효율지수 그리드 (SOSS.DM_CCTV_EFF_IDEX_GRID)
    spark_grid = SparkClass.spark.createDataFrame(cctv_eff_idex_grid_df)
    query_obj.insert_cctv_eff_idex_grid_sql(spark_grid)

    # CCTV 효율지수 행정동 (SOSS.DM_cctv_eff_idex_admd_df)
    tmp_need_cctv_df = cctv_eff_idex_grid_df.drop_duplicates(subset=["grid_id"]).groupby(by="admd_cd", as_index=False)["need_cctv_co"].sum()
    cctv_eff_idex_admd_df = cctv_eff_idex_grid_df.groupby(by=["stdr_de", "stdr_tm", "admd_cd"], as_index=False)["cctv_eff_idex"].mean()
    cctv_eff_idex_admd_df = cctv_eff_idex_admd_df.merge(tmp_need_cctv_df, how="left", on="admd_cd")
    cctv_eff_idex_admd_df["gu_cd"] = gu_cd

    spark_admd = SparkClass.spark.createDataFrame(cctv_eff_idex_admd_df)
    query_obj.insert_cctv_eff_idex_admd_sql(spark_admd)

    # CCTV 효율지수 시군구 (SOSS.DM_CCTV_EFF_IDEX_SGG)
    tmp_need_cctv_df = cctv_eff_idex_grid_df.drop_duplicates(subset=["grid_id"]).groupby(by="gu_cd", as_index=False)["need_cctv_co"].sum()
    cctv_eff_idex_sgg_df = cctv_eff_idex_grid_df.groupby(by=["stdr_de", "stdr_tm", "gu_cd"], as_index=False)["cctv_eff_idex"].mean()
    cctv_eff_idex_sgg_df = cctv_eff_idex_sgg_df.merge(tmp_need_cctv_df, how="left", on="gu_cd")

    spark_sgg = SparkClass.spark.createDataFrame(cctv_eff_idex_sgg_df)
    query_obj.insert_cctv_eff_idex_sgg_sql(spark_sgg)

def cctv_optmz_operation_main(arg_date: str, arg_gu_nm: str):

    global today, today_dt, gu_nm, gu_cd, query_obj

    # logging setting
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    logging.info("Main-Process : Start")
    start = time.time()

    logging.info(f"-- spark load --")

    # 충남 시군구 딕셔너리
    gu_dict: Dict[str, int] = {
        "gyeryong_si": 44250,
        "gongju_si": 44150,
        "geumsan_gun": 44710,
        "nonsan_si": 44230,
        "dangjin_si": 44270,
        "boryeong_si": 44180,
        "buyeo_gun": 44760,
        "seosan_si": 44210,
        "seocheon_gun": 44770,
        "asan_si": 44200,
        "yesan_gun": 44810,
        "cheongyang_gun": 44790,
        "taean_gun": 44825,
        "hongseong_gun": 44800,
        "cheonan_si_dongnam_gu": 44131,
        "cheonan_si_seobuk_gu": 44133
    }

    # .py 실행 파라미터로 현재 실행 날짜(YYYYMMDD)와 구이름이 들어옴
    today = arg_date
    gu_nm = arg_gu_nm
    gu_cd = gu_dict[gu_nm]

    # today를 datetime 타입으로 변환하여 저장하는 변수
    today_dt = datetime.strptime(today, "%Y%m%d")

    # cctv_optmz_query 인스턴스 생성
    query_obj = CctvOptmzQuery(today, gu_cd)

    # 01. 불안전지수 데이터
    unsafe_df = create_unsafe_idex()
    logging.info("-- create_unsafe_idex() is Finished --")
    logging.info(f"unsafe_df Count : {unsafe_df.shape[0]:,}")
    logging.info(f"unsafe_df Null Check\n{unsafe_df.isnull().sum()}\n")

    # 02. 토지 테이블 병합
    land_merge_df = merge_land(unsafe_df)
    logging.info("-- merge_land() is Finished --")
    logging.info(f"land_merge_df Count : {land_merge_df.shape[0]:,}")
    logging.info(f"land_merge_df Null Check\n{land_merge_df.isnull().sum()}\n")
    del unsafe_df

    # 03. 신고건수 테이블 병합
    report_merge_df = merge_report(land_merge_df)
    logging.info("-- merge_report() is Finished --")
    logging.info(f"report_merge_df Count : {report_merge_df.shape[0]:,}")
    logging.info(f"report_merge_df Null Check\n{report_merge_df.isnull().sum()}\n")
    del land_merge_df
    SparkClass.spark.catalog.clearCache()

    # 04. 여성 유동인구 예측 테이블 병합
    women_pop_merge_df = merge_women_pop(report_merge_df)
    logging.info("-- merge_women_pop() is Finished --")
    logging.info(f"women_pop_merge_df Count : {women_pop_merge_df.shape[0]:,}")
    logging.info(f"women_pop_merge_df Null Chek\n{women_pop_merge_df.isnull().sum()}\n")
    del report_merge_df
    SparkClass.spark.catalog.clearCache()

    # 05. cctv점수 테이블 병합
    cctv_merge_df = merge_cctv(women_pop_merge_df)
    logging.info("-- merge_cctv() is Finished --")
    logging.info(f"cctv_merge_df Count : {cctv_merge_df.shape[0]:,}")    
    logging.info(f"cctv_merge_df Null Check\n{cctv_merge_df.isnull().sum()}\n")
    del women_pop_merge_df

    # 기존 날짜에 데이터가 있다면 삭제
    delete_soss_cctv_eff_idex(today, gu_nm, gu_cd)
    
    # 06. 데이터 적재하기
    save_cctv_eff_idex(cctv_merge_df)
    logging.info("-- save_cctv_eff_idex() is Finished --")
    del cctv_merge_df

    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split('.')[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")
