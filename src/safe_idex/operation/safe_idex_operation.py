#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
안전지수 운영 모듈
"""

from __future__ import annotations

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import os
import time
from typing import (
    Tuple,
    List,
    Dict,    
    Union,
)

import joblib
import numpy as np
import pandas as pd
from pyspark.sql.utils import ParseException
from py4j.protocol import Py4JJavaError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from soss.spark_session.db_connector import SparkClass
from soss.queries.safe_idex_query import SafeIdexQuery
from soss.utils.config_parser import *

# import, export Folder
PREPROCESSING_DATA_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "preprocessing", "output")
MODEL_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "model_learning", "model")
MODEL_FOLD_CNT = 4

def delete_soss_safe_idex(today: str, gu_nm: str, gu_cd: int):
    
    # define delete date
    delete_date = datetime.strptime(today, "%Y%m%d") - relativedelta(days=2)
    delete_date = datetime.strftime(delete_date, "%Y%m%d")

    # Create an engine to connect to the database
    engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWRD}@{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}")

    # 세션을 사용하여 데이터베이스 작업 수행
    with Session(engine) as session:

        # 안전지수 - 그리드, 행정동, 시군구 테이블
        tables = (
            "SOSS.DM_SAFE_IDEX_GRID",
            "SOSS.DM_SAFE_IDEX_ADMD",
            "SOSS.DM_SAFE_IDEX_SGG"
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

def create_predict_df() -> pd.DataFrame:
    """01. 예측 데이터 셋을 만드는 함수

    Returns
    -------
    predict_df : pandas.DataFrame
    """

    def import_preprocessed() -> pd.DataFrame:
        """01-(1) 전처리 모듈에서 처리한 데이터를 불러오는 코드

        Returns
        -------
        preprocessed_df : pandas.DataFrame
        """

        # 전처리 전처리 데이터 로딩
        preprocessed_df = pd.read_csv(
            filepath_or_buffer=os.path.join(PREPROCESSING_DATA_FOLDER, f"{gu_nm}.csv"),
            dtype={
                "stdr_de": str,
                "stdr_tm": np.uint8,
                "grid_id": str,
                "admd_cd": str,
                "atp_val": np.float64,
                "hd_val": np.float64,
                "day_nm": np.uint8,
                "weak_score": np.float64
            }
        )

        return preprocessed_df

    def make_basic_var(preprocessed_df: pd.DataFrame) -> pd.DataFrame:
        """01-(2) : 기초 파생변수를 생성하는 함수

        Parameters
        ----------
        preprocessed_df : pandas.DataFrame
            기초 파생변수를 생성하려는 전처리 데이터

        Returns
        -------
        preprocessed_df : pandas.DataFrame
        """

        # 기준시간으로 그룹핑
        grouped_tm_df = preprocessed_df.groupby(by=["grid_id", "stdr_tm"], as_index=False).agg(tm_hazard_mean=("hazard_score", "mean"), tm_weak_mean=("weak_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_tm_df, how="left", on=["grid_id", "stdr_tm"])

        # 요일명으로 그룹핑
        grouped_day_nm_df = preprocessed_df.groupby(by=["grid_id", "day_nm"], as_index=False).agg(day_hazard_mean=("hazard_score", "mean"), day_weak_mean=("weak_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_day_nm_df, how="left", on=["grid_id", "day_nm"])

        # 습도값으로 그룹핑
        grouped_hd_val_df = preprocessed_df.groupby(by=["grid_id", "hd_val"], as_index=False).agg(hd_hazard_mean=("hazard_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_hd_val_df, how="left", on=["grid_id", "hd_val"])

        # 기온값으로 그룹핑
        grouped_atp_val_df = preprocessed_df.groupby(by=["grid_id", "atp_val"], as_index=False).agg(apt_hazard_mean=("hazard_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_atp_val_df, how="left", on=["grid_id", "atp_val"])

        # 행렬번호로 그룹핑
        grouped_grid_id_df = preprocessed_df.groupby(by="grid_id", as_index=False).agg(grid_hazard_mean=("hazard_score", "mean"), grid_weak_mean=("weak_score", "mean"), grid_weak_max=("weak_score", "max"))
        preprocessed_df = preprocessed_df.merge(grouped_grid_id_df, how="left", on="grid_id")

        preprocessed_df.drop(["atp_val", "hd_val"], axis=1, inplace=True)

        return preprocessed_df


    def split_df_by_date(preprocessed_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """01-(3) : 최소날짜와 최대날짜의 절반으로 데이터를 분할하는 함수

        Parameters
        ----------
        preprocessed_df : pandas.DataFrame
            전처리 모듈을 거친 구별 1년 단위의 데이터

        Returns
        -------
        split_date : str
            1년 단위의 데이터를 절반의 날짜로 분할하는 분리 날짜

        Notes
        -----
        preprocessed_df 데이터의 min, max 날짜값을 구해 절반 분할 후 하루를 뺀다
        """

        # start_date : 일자가 가장 빠른 일자
        start_date = datetime.strptime(preprocessed_df["stdr_de"].min(), "%Y%m%d")

        # end_date : 일자가 가장 느린 일자
        end_date = datetime.strptime(preprocessed_df["stdr_de"].max(), "%Y%m%d")

        # 두 날짜의 차이를 반으로 나눔
        diff_days = (end_date - start_date) / 2

        # start_date에 diff_days를 더하여 분리날짜를 반환
        split_date = (start_date + diff_days).strftime("%Y%m%d")

        # 통계용 파생변수 데이터 생성
        statistic_df = preprocessed_df.query("stdr_de < @split_date").copy()
        statistic_df.drop("stdr_de", axis=1, inplace=True)
        statistic_df.reset_index(drop=True, inplace=True)


        # 학습용 파생 변수 데이터셋 분리
        train_df = preprocessed_df.query("stdr_de >= @split_date").copy()
        train_df.drop("stdr_de", axis=1, inplace=True)
        train_df.reset_index(drop=True, inplace=True)

        return statistic_df, train_df

    def create_grid() -> pd.DataFrame:
        """01-(4) : 오늘 날짜 그리드를 생성하는 함수

        Returns
        -------
        predict_df : pandas.DataFrame
            오늘 날짜의 그리드 데이터
        """

        # 공간격자 데이터 불러오기
        try:
            grid_df = query_obj.get_pcel_stdr_info_sql()
            if not grid_df.take(1):
                Exception("Fucntion : get_pcel_stdr_info_sql() 쿼리로 가져오는 데이터가 없습니다")
        except (ParseException, Py4JJavaError) as e:
            logging.error(str(e))
        else:
            grid_df = grid_df.toPandas()

        # grid_tm_df
        grid_tm_df = pd.DataFrame()

        for tm in range(0, 24):
            grid_copy_df = grid_df.copy()
            grid_copy_df["stdr_tm"] = np.uint8(tm)
            grid_tm_df = pd.concat([grid_tm_df, grid_copy_df], ignore_index=True)

        # 날짜 특성정보 투입
        grid_tm_df["day_nm"] = datetime.strptime(today, "%Y%m%d").weekday()

        # 컬럼 순서 정리
        grid_tm_df = grid_tm_df[["grid_id", "stdr_tm", "day_nm", "admd_cd"]]

        return grid_tm_df

    def merge_statistic(statistic_df: pd.DataFrame, null_df: pd.DataFrame, grouping: Union[List[str], str], derived_var: List[str], target_col=str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """변수들 묶음을 인풋으로 받아 그룹핑하여, 결측 값 유무에 따라 데이터프레임을 분할하여 리턴해주는 함수

        Parameters
        ----------
        statistic_df : pandas.DataFrame
            통계 파생 변수 생성에 사용할 전처리 데이터
        null_df : pandas.DataFrame
            통계 파생 변수를 붙일 그리드 데이터
        grouping : List[str]
            그룹핑할 변수들 묶음(순서 상관 있음)

        Returns
        -------
        train_df_part : pandas.DataFrame
            group별 생성할 파생변수 값이 결측값 없이 채워진 데이터프레임
        null_df : pandas.DataFrame
            group별 생성할 파생변수 값이 결측값으로 채워진 데이터프레임
        """

        # group으로 그룹핑하여 변수의 평균값 생성
        grouped_df = statistic_df.groupby(by=grouping, as_index=False)[derived_var].mean()

        # 그룹핑된 데이터프레임을 파생변수가 없는 행들과 결합하기
        merge_df = null_df.merge(right=grouped_df, how="left", on=grouping).fillna(-1)

        # 결측치가 없는 데이터프레임은 병합
        train_df_part = merge_df[merge_df[target_col] != -1]

        # 병합에 사용된 데이터프레임 변수 제거 (다음 groupby에 활용하기 위해 groupby로 생성된 파생변수 제거)
        null_df = merge_df[merge_df[target_col] == -1].copy()
        null_df.drop(labels=derived_var, axis=1, inplace=True)

        return train_df_part, null_df

    def make_hazard_predict_df(statistic_df : pd.DataFrame, predict_df: pd.DataFrame) -> pd.DataFrame:
        """01-(5) : 위해지표 파생변수를 생성하는 함수

        Parameters
        ----------
        statistic_df : pandas.DataFrame
            통계 파생 변수 데이터 셋
        predict_df : pandas.DataFrame
            모델 예측 데이터 셋

        Returns
        -------
        predict_weak : pandas.DataFrame
            파생변수를 붙인 후 모델링에 필요한 컬럼만 추출한 데이터

        Notes
        -----
        랜덤샘플링으로 인해 grid_id, stdr_tm, day_nm 조건을 전부 충족하는 데이터가 없는 경우
        grid_id/stdr_tm/day_nm -> grid_id/stdr_tm -> grid_id/day_nm -> grid_id -> admd_cd 순으로 N/A값을 채운다
        """

        statistic_df = statistic_df[["grid_id", "stdr_tm", "day_nm", "admd_cd", "tm_hazard_mean", "day_hazard_mean", "hd_hazard_mean", "apt_hazard_mean", "grid_hazard_mean"]]

        # 생성할 파생변수 리스트
        derived_var = ["tm_hazard_mean", "day_hazard_mean", "hd_hazard_mean", "apt_hazard_mean", "grid_hazard_mean"]

        # 통계 값을 붙인 데이터프레임 파트들을 담을 리스트
        hazard_predict_df_parts = []

        # 행렬번호별-기준시간별-요일별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
        hazard_predict_df_part1, null_df = merge_statistic(statistic_df, predict_df, ["grid_id", "stdr_tm", "day_nm"], derived_var, "apt_hazard_mean")
        hazard_predict_df_parts.append(hazard_predict_df_part1)

        if not null_df.empty:
            # 행렬번호별-기준시간별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
            hazard_predict_df_part2, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "stdr_tm"], derived_var, "apt_hazard_mean")
            hazard_predict_df_parts.append(hazard_predict_df_part2)

        if not null_df.empty:
            # 행렬번호별-요일별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
            hazard_predict_df_part3, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "day_nm"], derived_var, "apt_hazard_mean")
            hazard_predict_df_parts.append(hazard_predict_df_part3)

        if not null_df.empty:
            # 행렬번호별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
            hazard_predict_df_part4, null_df = merge_statistic(statistic_df, null_df, "grid_id", derived_var, "apt_hazard_mean")
            hazard_predict_df_parts.append(hazard_predict_df_part4)

        if not null_df.empty:
            # 행정동 코드별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
            hazard_predict_df_part5, _ = merge_statistic(statistic_df, null_df, "admd_cd", derived_var, "apt_hazard_mean")
            hazard_predict_df_parts.append(hazard_predict_df_part5)

        hazard_predict_df = pd.concat(hazard_predict_df_parts, ignore_index=True)
        hazard_predict_df.drop(["admd_cd", "day_nm"], axis=1, inplace=True)

        return hazard_predict_df

    def make_weak_predict_df(statistic_df: pd.DataFrame, predict_df: pd.DataFrame) -> pd.DataFrame:
        """01-(6) : weak(취약지표) 파생변수 생성하는 함수

        Parameters
        ----------
        statistic_df : pandas.DataFrame
            통계 파생 변수 데이터 셋
        predict_df : pandas.DataFrame
            모델 예측 데이터 셋

        Returns
        -------
        predict_weak : pandas.DataFrame
            group_var별 생성할 파생변수 값이 결측값 없이 채워진 데이터프레임
        null_df : pandas.DataFrame
            group_var별 생성할 파생변수 값이 결측값으로 채워진 데이터프레임

        Notes
        -----
        랜덤샘플링으로 인해 grid_id, stdr_tm, day_nm 조건을 전부 충족하는 데이터가 없는 경우
        grid_id/stdr_tm/day_nm -> grid_id/tm -> grid_id/day_nm -> grid_id -> admd_cd 순으로 N/A값을 채운다
        """

        statistic_df = statistic_df[["grid_id", "stdr_tm", "day_nm", "admd_cd", "tm_weak_mean", "day_weak_mean", "grid_weak_mean", "grid_weak_max"]]

        # 생성할 파생변수 리스트
        derived_var = ["tm_weak_mean", "day_weak_mean", "grid_weak_mean", "grid_weak_max"]

        # 통계 값을 붙인 데이터프레임 파트들을 담을 리스트
        weak_predict_df_parts = []

        # 행렬번호별-기준시간별-요일별 (시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값)들의 평균
        weak_predict_df_part1, null_df = merge_statistic(statistic_df, predict_df, ["grid_id", "stdr_tm", "day_nm"], derived_var, "tm_weak_mean")
        weak_predict_df_parts.append(weak_predict_df_part1)

        if not null_df.empty:
            # 행렬번호별-기준시간별 (시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값)들의 평균
            weak_predict_df_part2, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "stdr_tm"], derived_var, "tm_weak_mean")
            weak_predict_df_parts.append(weak_predict_df_part2)

        if not null_df.empty:
            # 행렬번호별-요일별 (시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값)들의 평균
            weak_predict_df_part3, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "day_nm"], derived_var, "tm_weak_mean")
            weak_predict_df_parts.append(weak_predict_df_part3)

        if not null_df.empty:
            # 행렬번호별 (시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값)들의 평균
            weak_predict_df_part4, null_df = merge_statistic(statistic_df, null_df, "grid_id", derived_var, "tm_weak_mean")
            weak_predict_df_parts.append(weak_predict_df_part4)

        if not null_df.empty:
            # 행정동 코드별 (시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값)들의 평균
            weak_predict_df_part5, _ = merge_statistic(statistic_df, null_df, "admd_cd", derived_var, "tm_weak_mean")
            weak_predict_df_parts.append(weak_predict_df_part5)

        weak_predict_df = pd.concat(weak_predict_df_parts, ignore_index=True)
        weak_predict_df.drop(["admd_cd", "day_nm"], axis=1, inplace=True)

        return weak_predict_df

    # 01-(1) data_preprocessing.py 에서 전처리된 데이터 불러오기
    preprocessed_df = import_preprocessed()

    # 01-(2) 기초 파생변수 생성
    preprocessed_df = make_basic_var(preprocessed_df)

    # 01-(3) 중간 날짜로 데이터 분리
    _, statistic_df = split_df_by_date(preprocessed_df)

    # 01-(4) 예측 데이터셋 생성
    predict_df = create_grid()

    # 01-(5) 위해지표 예측 데이터 생성
    hazard_predict_df = make_hazard_predict_df(statistic_df, predict_df)

    # 01-(6) 취약지표 예측 데이터 생성
    weak_predict_df = make_weak_predict_df(statistic_df, predict_df)

    return hazard_predict_df, weak_predict_df

def predict_hazard(hazard_predict_df: pd.DataFrame) -> pd.DataFrame:
    """02. 위해지표를 예측하는 함수

    Parameters
    ----------
    hazard_predict_df : pandas.DataFrame
       위해지표 파생변수 생성이 끝난 데이터

    Returns
    -------
    hazard_predict_df : pandas.DataFrame
       위해지표 예측값을 merge한 데이터

    Notes
    -----
    Training 모듈에서 저장한 joblib파일을 불러와 예측할 날짜의 위해지표를 예측
    """

    # Dataset 분할
    pred_X = hazard_predict_df[["tm_hazard_mean", "day_hazard_mean", "hd_hazard_mean", "apt_hazard_mean", "grid_hazard_mean"]].values

    # 최적 학습모델 로딩
    best_model = {}
    hazard_score = 0

    for idx in range(MODEL_FOLD_CNT):
        best_model[idx] = joblib.load(os.path.join(MODEL_FOLDER, "hazard", gu_nm, f"{gu_nm}_{idx}.pkl"))

    for idx in best_model:
        hazard_score += (best_model[idx].predict(pred_X) / len(best_model))


    # 최적 모델 예측값 테이블 적재 (위해지표)
    hazard_predict_df["hazard_score"] = np.where(hazard_score < 0, 0, hazard_score)
    hazard_predict_df["hazard_score"] = hazard_predict_df["hazard_score"] - hazard_predict_df["hazard_score"].min()
    hazard_predict_df["hazard_score"] = round((hazard_predict_df["hazard_score"] / hazard_predict_df["hazard_score"].max()) * 65, 2)

    hazard_predict_df = hazard_predict_df[["grid_id", "stdr_tm", "hazard_score"]]

    return hazard_predict_df

def predict_weak(weak_predict_df: pd.DataFrame) -> pd.DataFrame:
    """03. 취약지표를 예측하는 함수

    Parameters
    ----------
    weak_predict_df : pandas.DataFrame
       취약지표 파생변수 생성이 끝난 데이터

    Returns
    -------
    test_weak : pandas.DataFrame
       취약지표 예측값을 merge한 데이터

    Notes
    -----
    Training 모듈에서 저장한 joblib파일을 불러와 예측할 날짜의 취약지표 변수를 예측
    """

    # Dataset 분할
    pred_X = weak_predict_df[["tm_weak_mean", "day_weak_mean", "grid_weak_mean", "grid_weak_max"]].values

    # 최적 학습모델 로딩
    best_model = {}
    weak_score = 0

    for idx in range(MODEL_FOLD_CNT):
        best_model[idx] = joblib.load(os.path.join(MODEL_FOLDER, "weak", gu_nm, f"{gu_nm}_{idx}.pkl"))

    for idx in best_model:
        weak_score += (best_model[idx].predict(pred_X)/len(best_model))

    # 최적 모델 예측값 테이블 적재 (취약지표)
    weak_predict_df["weak_score"] = np.where(weak_score < 0, 0, weak_score)
    weak_predict_df["weak_score"] = weak_predict_df["weak_score"] - weak_predict_df["weak_score"].min()
    weak_predict_df["weak_score"] = round((weak_predict_df["weak_score"] / weak_predict_df["weak_score"].max()) * 18, 2)

    weak_predict_df = weak_predict_df[["grid_id", "stdr_tm", "weak_score"]]

    return weak_predict_df

def calculate_mitigation() -> pd.DataFrame:
    """04. 경감지표(CCTV)를 산출하는 함수

    Returns
    -------
    cctv_df : pandas.DataFrame
    """

    # CCTV 데이터 불러오기
    try:
        cctv_df = query_obj.get_pcel_cctv_cnt_sql()
        if not cctv_df.take(1):
            raise Exception("Fucntion : get_ic_pcel_cctv_cnt_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        cctv_df = cctv_df.toPandas()

    cctv_df["mitigation_score"] = np.where(cctv_df["cctv_co"] > 6, 25, cctv_df["cctv_co"] * 4)
    cctv_df["mitigation_score"] = cctv_df["mitigation_score"] - cctv_df["mitigation_score"].min()
    cctv_df["mitigation_score"] = round(cctv_df["mitigation_score"] / cctv_df["mitigation_score"].max() * 17, 2)

    cctv_df = cctv_df[["grid_id", "admd_cd", "mitigation_score"]]

    return cctv_df

def save_safe_idex(harzard_predict_df: pd.DataFrame, weak_predict_df: pd.DataFrame, cctv_df: pd.DataFrame):
    """05. 안전지수 산출 및 DB적재 함수

    Parameters
    ----------
    harzard_predict_df : pandas.DataFrame
       위해지표(112신고) 예측값이 포함된 데이터

    weak_predict_df : pandas.DataFrame
       취약지표(유동인구) 예측값이 포함된 데이터

    cctv_df : pandas.DataFrame
       경감지표(CCTV) 점수가 반영된 데이터
    """

    # 위해지표(112 신고) 데이터, 취약지표(SKT 유동인구) 예측 데이터, CCTV 경감지표 점수 데이터 결합하기
    safe_idex_grid_df = harzard_predict_df.merge(weak_predict_df, how="left", on=["grid_id", "stdr_tm"])
    safe_idex_grid_df = safe_idex_grid_df.merge(cctv_df, how="left", on="grid_id")

    # 안전지수 = 100 - 위해지표 - 취약지표 + 경감지표
    safe_idex_grid_df["safe_idex"] = 100 - safe_idex_grid_df["hazard_score"] - safe_idex_grid_df["weak_score"] + safe_idex_grid_df["mitigation_score"]
    safe_idex_grid_df.drop(["hazard_score", "weak_score", "mitigation_score"], axis=1, inplace=True)

    safe_idex_grid_df["safe_idex"] = safe_idex_grid_df["safe_idex"] - safe_idex_grid_df["safe_idex"].min()
    safe_idex_grid_df["safe_idex"] = round(safe_idex_grid_df["safe_idex"] / safe_idex_grid_df["safe_idex"].max() * 100, 2)

    # 일자와 구 코드 기입
    safe_idex_grid_df["stdr_de"] = today
    safe_idex_grid_df["gu_cd"] = gu_cd
    logging.info(f"safe_idex_grid_df Count : {safe_idex_grid_df.shape[0]:,}")
    logging.info(f"safe_idex_grid_df Null Check\n{safe_idex_grid_df.isnull().sum()}\n")

    # 시간 두자리 수로 채우기
    safe_idex_grid_df["stdr_tm"] = safe_idex_grid_df["stdr_tm"].apply(lambda x: str(x).zfill(2))
    safe_idex_grid_df.fillna(0, inplace=True)

    # 안전지수 그리드 (SOSS.DM_SAFE_IDEX_GRID)
    spark_grid = SparkClass.spark.createDataFrame(safe_idex_grid_df)
    query_obj.insert_safe_idex_grid_sql(spark_grid)

    # 안전지수 행정동 (SOSS.DM_SAFE_IDEX_ADMD)
    safe_idex_admd_df = safe_idex_grid_df.groupby(["stdr_de", "stdr_tm", "admd_cd"], as_index=False)["safe_idex"].mean()
    safe_idex_admd_df["gu_cd"] = gu_cd

    spark_admd = SparkClass.spark.createDataFrame(safe_idex_admd_df)
    query_obj.insert_safe_idex_admd_sql(spark_admd)

    # 안전지수 시군구 (SOSS.DM_SAFE_IDEX_SGG)
    safe_idex_sgg_df = safe_idex_grid_df.groupby(["stdr_de", "stdr_tm", "gu_cd"], as_index=False)["safe_idex"].mean()

    spark_sgg = SparkClass.spark.createDataFrame(safe_idex_sgg_df)
    query_obj.insert_safe_idex_sgg_sql(spark_sgg)

def safe_idex_operation_main(arg_date: str, arg_gu_nm: str):

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

    # 구별 반복문 시행
    logging.info(f"{gu_nm} Start --\n")

    # safe_idex_query 인스턴스 생성
    query_obj = SafeIdexQuery(today, gu_cd)

    # 01. 통계용 데이터 생성
    hazard_predict_df, weak_predict_df = create_predict_df()
    logging.info("-- create_predict_df() is Finished --")
    logging.info(f"hazard_predict_df Count : {hazard_predict_df.shape[0]:,}")
    logging.info(f"hazard_predict_df Null Check\n{hazard_predict_df.isnull().sum()}\n")

    logging.info(f"weak_predict_df Count : {hazard_predict_df.shape[0]:,}")
    logging.info(f"weak_predict_df Null Check\n{weak_predict_df.isnull().sum()}\n")

    # 02. 위해지표 예측
    harzard_predict_df = predict_hazard(hazard_predict_df)
    logging.info("-- predict_hazard() is Finished --")
    logging.info(f"hazard_predict_df Count : {hazard_predict_df.shape[0]:,}")
    logging.info(f"hazard_predict_df Null Check\n{harzard_predict_df.isnull().sum()}\n")    

    # 03. 취약지표 예측
    weak_predict_df = predict_weak(weak_predict_df)
    logging.info("-- predict_weak() is Finished --")
    logging.info(f"weak_predict_df Count : {hazard_predict_df.shape[0]:,}")
    logging.info(f"weak_predict_df Null Check\n{weak_predict_df.isnull().sum()}\n")    

    # 04. 경감지표 산출
    cctv_df = calculate_mitigation()
    logging.info("-- calculate_mitigation() is Finished --")
    logging.info(f"cctv_df Count : {cctv_df.shape[0]:,}")
    logging.info(f"cctv_df Null Check\n{cctv_df.isnull().sum()}\n")

    # 기존 날짜에 데이터가 있다면 삭제
    delete_soss_safe_idex(today, gu_nm, gu_cd)

    # 05. 데이터 적재하기
    save_safe_idex(harzard_predict_df, weak_predict_df, cctv_df)

    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split('.')[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")