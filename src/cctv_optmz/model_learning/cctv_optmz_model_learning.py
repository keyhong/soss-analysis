#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import logging
import os
import sys
import time
from typing import Tuple

from dateutil.relativedelta import relativedelta
import joblib
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_squared_error

from soss.queries.cctv_optmz_query import CctvOptmzQuery
from soss.spark_session.db_connector import SparkClass

# import, export Folder
MODEL_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model")
SCALER_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scaler")

MONTH_PERIOD = 2
MONTH_PERIOD_HALF = MONTH_PERIOD // 2

def create_train_df() -> pd.DataFrame:
    """01. 여성 유동인구 데이터를 로딩하는 함수

    Returns
    -------
    train_df : pandas.DataFrame
       학습 데이터셋
    """

    for year in range(0, 5):

        # 일자 설정
        start_date = (today_dt - relativedelta(years=year, months=MONTH_PERIOD_HALF)).strftime("%Y%m%d")
        end_date = (today_dt - relativedelta(years=year, days=1)).strftime("%Y%m%d")

        # (1개월 전 ~ 어제) 통계 데이터를 불러오는 코드
        train_df = query_obj.get_train_women_pop_sql(start_date, end_date)

        if train_df.take(1):
            break

    # 요일 변수 생성
    train_df = train_df.withColumn("weekday", F.to_date(F.unix_timestamp("stdr_de", "yyyyMMdd").cast("timestamp")))
    train_df = train_df.withColumn("weekday", F.date_format("weekday", 'E'))

    # 학습용 여성 유동인구 수 데이터 로딩 함수 실행
    train_df = train_df.toPandas()
    
    train_df["stdr_tm"] = train_df["stdr_tm"].astype("UInt8")
    
    return train_df

def merge_statistic_df(train_df: pd.DataFrame) -> pd.DataFrame:
    """02. 통계용 여성 유동인구 데이터 셋을 결합하는 함수

    Returns
    -------
    train_df : pandas.DataFrame
       학습 데이터
    """

    for year in range(0, 5):
        # 일자 설정
        start_date = (today_dt - relativedelta(years=year, months=MONTH_PERIOD)).strftime("%Y%m%d")
        end_date = (today_dt - relativedelta(years=year, months=MONTH_PERIOD_HALF, days=1)).strftime("%Y%m%d")

        # (2개월 전 ~ 1개월 1일 전) 통계 데이터를 불러오는 코드
        statistic_df = query_obj.get_statistic_women_pop_sql(start_date, end_date)

        if statistic_df.take(1):
            break

    # 통계용 데이터 요일 컬럼 생성
    statistic_df = statistic_df.withColumn("weekday", F.to_date(F.unix_timestamp("stdr_de", "yyyyMMdd").cast("timestamp")))
    statistic_df = statistic_df.withColumn("weekday", F.date_format("weekday", 'E'))

    # 그리드별-시간별-요일별 여성유동인구 평균, 그리드별-시간별-요일별 여성유동인구 최댓값
    statistic_df = statistic_df.groupby(["grid_id", "stdr_tm", "weekday"]).agg(F.mean("women_pop").alias("women_pop_mean"), F.max("women_pop").alias("women_pop_max"))
    statistic_df = statistic_df.toPandas()
    
    statistic_df["stdr_tm"] = statistic_df["stdr_tm"].astype("UInt8")

    # 통계용 데이터와 학습용 데이터 inner join
    train_df = train_df.merge(statistic_df, how="inner", on=["grid_id", "stdr_tm", "weekday"])

    return train_df

def merge_bus_stop_df(train_df: pd.DataFrame) -> pd.DataFrame:
    """03. 버스정류소 개수 데이터 셋을 결합하는 함수

    Returns
    -------
    train_df : pandas.DataFrame
       학습 데이터
    """

    # 버스정류소 개수 데이터 로딩
    bus_stop_cnt_df = query_obj.get_bus_stop_cnt_sql()
    
    if not bus_stop_cnt_df.take(1):
        logging.error("bus_stop_cnt_df has no data")
    
    bus_stop_cnt_df = bus_stop_cnt_df.toPandas()

    train_df = train_df.merge(bus_stop_cnt_df, how="left", on="grid_id")
    train_df["bus_stop_cnt"].fillna(0, inplace=True)

    return train_df

def apply_get_dummies(train_df: pd.DataFrame):
    
    # 범주형 변수(요일) One-Hot Encoding
    train_df = pd.get_dummies(train_df, columns=["weekday"])

    # 없는 요일 범주형 변수 만들기
    days = [ "weekday_Mon", "weekday_Tue", "weekday_Wed", "weekday_Thu", "weekday_Fri", "weekday_Sat", "weekday_Sun" ]

    for day in filter(lambda x: x not in train_df.columns, days):
        train_df[day] = np.uint8(0)
        
    cols = [
        "stdr_de",
        "stdr_tm",
        "grid_id",
        "women_pop",
        "women_pop_mean",
        "women_pop_max",
        "bus_stop_cnt"
    ]
    
    cols.extend(days)
    
    train_df = train_df.reindex(columns=cols)
    
    return train_df

def split_train_test(dataFrame: pd.DataFrame)-> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """04. 학습용 데이터, 테스트용 데이터를 분리하는 함수

    Parameters
    ----------
    dataFrame : pandas.DataFrame
       학습 데이터

    Returns
    -------
    train_x : pandas.DataFrame
       모델 학습 독립변수 데이터 

    test_x : pandas.DataFrame
       모델 학습 종속변수 데이터 

    train_y : pandas.DataFrame
       모델 테스트 독립변수 데이터 

    test_y : pandas.DataFrame
       모델 테스트 종속변수 데이터 
    """

    # 학습 데이터, 테스트 데이터 분리하기
    x = dataFrame[["women_pop_mean", "women_pop_max", "bus_stop_cnt",
                   "weekday_Mon", "weekday_Tue", "weekday_Wed", "weekday_Thu", "weekday_Fri", "weekday_Sat", "weekday_Sun"]]
    
    y = dataFrame[["women_pop"]]

    # 데이터 분리하기 
    train_x, test_x, train_y, test_y = train_test_split(x, y, test_size=0.25, random_state=42)

    return train_x, test_x, train_y, test_y

def store_min_max_sacler(train_x: pd.DataFrame):
    
    # MinMaxScaler 불러오기
    min_max_scaler = MinMaxScaler()

    # 연속형 변수 MinMaxScaler Fit & Transform
    train_x[["women_pop_mean", "women_pop_max", "bus_stop_cnt"]] =  min_max_scaler.fit_transform(train_x[["women_pop_mean", "women_pop_max", "bus_stop_cnt"]])

    # Save MinMaxScaler
    joblib.dump(min_max_scaler, os.path.join(SCALER_FOLDER, f"{gu_nm}_scaler.pkl"))
    
    return train_x

def train_women_pop_model(train_x: pd.DataFrame, train_y: pd.DataFrame):
    """05. 학습용 데이터를 학습시키는 함수

    Parameters
    ----------
    train_x : pandas.DataFrame
       모델 학습 독립변수 데이터 

    train_y : pandas.DataFrame
       모델 테스트 독립변수 데이터      
    """

    # 모델 생성 및 학습
    logging.info(f"RandomForestRegressor {gu_nm} 학습시작 --")
    rf = RandomForestRegressor(max_depth=6, n_jobs=16)
    rf.fit(train_x, train_y)

    # Save RandomForest Regreesion Model
    joblib.dump(rf, os.path.join(MODEL_FOLDER, f"{gu_nm}_rf.pkl"))

def test_women_pop(test_x: pd.DataFrame, test_y: pd.DataFrame):
    """05. 테스트 데이터를 평가하는 함수

    Parameters
    ----------
    dataFrame : pandas.DataFrame
       학습 데이터

    Returns
    -------
    test_x : pandas.DataFrame
       모델 학습 종속변수 데이터 

    test_y : pandas.DataFrame
       모델 테스트 종속변수 데이터 
    """
    
    # Load MinMaxScaler
    min_max_scaler = joblib.load(os.path.join(SCALER_FOLDER, f"{gu_nm}_scaler.pkl"))

    # 연속형 변수 MinMaxScaler Transform
    test_x[["women_pop_mean", "women_pop_max", "bus_stop_cnt"]] =  min_max_scaler.transform(test_x[["women_pop_mean", "women_pop_max", "bus_stop_cnt"]])

    # Load RandomForest Regression Model
    rf = joblib.load(os.path.join(MODEL_FOLDER, f"{gu_nm}_rf.pkl"))

    # 모델 예측
    test_pred = rf.predict(test_x)

    # 평가 지표
    r2 = r2_score(test_y, test_pred)
    logging.info(f"R2-SCORE : {round(r2, 2)}")
    
    mse = mean_squared_error(test_y, test_pred)
    logging.info(f"MSE : {round(mse, 2)}")

    rmse = mean_squared_error(test_y, test_pred, squared=False)
    logging.info(f"RMSE : {round(rmse, 2)}")
    

def cctv_optmz_model_learning_main(arg_date: str, arg_gu_nm: str):

    global today, today_dt, gu_nm, query_obj

    # logging setting
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    logging.info("Main-Process : Start")
    start = time.time()
    
    logging.info(f"-- spark load --")

    # 충남 시군구 딕셔너리 
    gu_dict = {
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

    # 01. 훈련 데이터 가져오기
    train_df = create_train_df()
    logging.info("-- create_train_df() 종료 --")
    SparkClass.spark.catalog.clearCache()

    # 02. 통계 데이터 결합하기
    train_df = merge_statistic_df(train_df)
    logging.info("-- merge_statistic_df() 종료 --")
    SparkClass.spark.catalog.clearCache()

    # 03. 버스정류소 개수 데이터 결합하기
    train_df = merge_bus_stop_df(train_df)
    logging.info("-- merge_bus_stop_df() 종료 --")
    
    """
    # 랜덤 샘플링 적용
    train_df = train_df.sample(frac=0.25)
    logging.info("-- random sampling 적용 --")
    """
    
    # 04. 요일에 적용 dummy
    train_df = apply_get_dummies(train_df)
    logging.info("-- apply_get_dummies() 종료 --")
    
    # 05. 데이터 분리하기
    train_x, test_x, train_y, test_y = split_train_test(train_df)
    logging.info("-- split_train_test() 종료 --")
    del train_df
    
    # 06. 스케일러 저장하기
    train_x = store_min_max_sacler(train_x)
    logging.info("-- store_min_max_sacler() 종료 --")

    # 07. 모델 생성
    train_women_pop_model(train_x, train_y)
    logging.info("-- train_women_pop_model() 종료 --")                
    del train_x, train_y
    
    # 08. 모델 테스트
    test_women_pop(test_x, test_y)
    del test_x, test_y
    
    SparkClass.spark.stop()
    logging.info(f"{gu_nm} End --")
    
    time.sleep(10)
    
    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split(".")[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")