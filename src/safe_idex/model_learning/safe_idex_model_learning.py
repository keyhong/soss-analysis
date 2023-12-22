#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
안전지수 학습 모듈
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
import time
from typing import (
    List,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
from sklearn.model_selection import ShuffleSplit

from soss.safe_idex.model_learning.model_factory import (
    ModelFactory,
    XGBoost,
    LGBM
)

PREPROCESSING_DATA_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "preprocessing", "output")

MODEL_FOLD_CNT = 4
MONTH_PERIOD = 2

def create_train_df() -> pd.DataFrame:
    """01. 학습 데이터 셋을 생성하는 함수

    Returns
    -------
    train_df : pandas.DataFrame
       학습 데이터셋
    """

    def import_preprocessed() -> pd.DataFrame:
        """01-(1) safe_idex_preprocessing.py에서 전처리된 구별 parquet파일을 로딩하는 함수

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
                "hd_val" : np.float64,
                "day_nm": str,
                "weak_score": np.float64
            }
        )
        
        return preprocessed_df

    def make_basic_var(preprocessed_df: pd.DataFrame) -> pd.DataFrame:
        """01-(2) 기초 파생변수를 생성하는 함수

        Parameters
        ----------
        preprocessed_df : pandas.DataFrame
        기초 파생변수를 생성하려는 전처리 데이터

        Returns
        -------
        preprocessed_df : pandas.DataFrame

        Notes
        -----
        행렬번호-컬럼별로 그룹핑을 하여 파생변수를 추가한다
        """

        # 그리드별-시간별 위해지표 평균, 그리드별-시간별 취약지표 평균
        grouped_tm_df = preprocessed_df.groupby(by=["grid_id", "stdr_tm"], as_index=False).agg(tm_hazard_mean=("hazard_score", "mean"), tm_weak_mean=("weak_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_tm_df, how="left", on=["grid_id", "stdr_tm"])

        # 그리드별-요일별 위해지표 평균, 그리드별-요일별 취약지표 평균
        grouped_day_nm_df = preprocessed_df.groupby(by=["grid_id", "day_nm"], as_index=False).agg(day_hazard_mean=("hazard_score", "mean"), day_weak_mean=("weak_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_day_nm_df, how="left", on=["grid_id", "day_nm"])

        # 그리드별-습도값별 위해지표 평균
        grouped_hd_val_df = preprocessed_df.groupby(by=["grid_id", "hd_val"], as_index=False).agg(hd_hazard_mean=("hazard_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_hd_val_df, how="left", on=["grid_id", "hd_val"])

        # 그리드별-기온값별 위해지표 평균
        grouped_df_atp_val = preprocessed_df.groupby(by=["grid_id", "atp_val"], as_index=False).agg(apt_hazard_mean=("hazard_score", "mean"))
        preprocessed_df = preprocessed_df.merge(grouped_df_atp_val, how="left", on=["grid_id", "atp_val"])

        # 그리드별 위해지표 평균, 그리드별 취약지표 평균, 그리드별 취약지표 최댓값
        grouped_grid_id_df = preprocessed_df.groupby(by="grid_id", as_index=False).agg(grid_hazard_mean=("hazard_score", "mean"), grid_weak_mean=("weak_score", "mean"), grid_weak_max=("weak_score", "max"))
        preprocessed_df = preprocessed_df.merge(grouped_grid_id_df, how="left", on="grid_id")

        preprocessed_df.drop(["hd_val", "atp_val"], axis=1, inplace=True)

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
        statistic_df = preprocessed_df[preprocessed_df["stdr_de"] < split_date].copy()
        statistic_df.reset_index(drop=True, inplace=True) 
        statistic_df.drop("stdr_de", axis=1, inplace=True)
        
        # 학습용 파생 변수 데이터셋 분리
        train_df = preprocessed_df[preprocessed_df["stdr_de"] >= split_date].copy()
        train_df.reset_index(drop=True, inplace=True)
        train_df.drop("stdr_de", axis=1, inplace=True)

        return statistic_df, train_df


    def merge_statistic(statistic_df: pd.DataFrame, null_df: pd.DataFrame, grouping: Union[List[str], str], target: List[str], null_check_col=str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """01-(4), (5) - inner function : 그룹핑한 값을 결합하여, 결측 행 데이터를 분리하는 함수

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
        grouped_df = statistic_df.groupby(by=grouping, as_index=False)[target].mean()

        # 그룹핑된 데이터프레임을 파생변수가 없는 행들과 결합하기
        merge_df = null_df.merge(right=grouped_df, how="left", on=grouping).fillna(-1)

        # 결측치가 없는 데이터프레임은 병합
        train_df_part = merge_df[merge_df[null_check_col] != -1]

        # 병합에 사용된 데이터프레임 변수 제거 (다음 groupby에 활용하기 위해 groupby로 생성된 파생변수 제거)
        null_df = merge_df[merge_df[null_check_col] == -1].copy()
        null_df.drop(labels=target, axis=1, inplace=True)

        return train_df_part, null_df

    def make_hazard_train_df(statistic_df: pd.DataFrame, train_df: pd.DataFrame) -> pd.DataFrame:
        """01-(4) : 위해지표(harzard_score) 학습 데이터를 생성하는 함수

        Parameters
        ----------
        statistic_df : pandas.DataFrame
        통계 파생 변수 데이터 셋
        train_df : pandas.DataFrame
        모델 학습 데이터 셋

        Returns
        -------
        hazard_train_df : pandas.DataFrame
        위해지표 모델 학습시킬 데이터

        Notes
        -----
        랜덤샘플링으로 인해 grid_id, stdr_tm, day_nm 조건을 전부 충족하는 데이터가 없는 경우
        grid_id/stdr_tm/day_nm -> grid_id/stdr_tm -> grid_id/day_nm -> grid_id -> admd_cd 순으로 N/A값을 채운다
        """

        statistic_df = statistic_df[["grid_id", "stdr_tm", "admd_cd", "day_nm", "tm_hazard_mean", "day_hazard_mean", "hd_hazard_mean", "apt_hazard_mean", "grid_hazard_mean"]]
        train_df = train_df[["grid_id", "stdr_tm", "admd_cd", "day_nm", "hazard_score"]]
        
        # 생성할 파생변수 리스트
        target = ["tm_hazard_mean", "day_hazard_mean", "hd_hazard_mean", "apt_hazard_mean", "grid_hazard_mean"]

        # 그리드별-시간별-요일별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
        hazard_train_df_part1, null_df = merge_statistic(statistic_df, train_df, ["grid_id", "stdr_tm", "day_nm"], target, "apt_hazard_mean")

        # 그리드별-시간별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
        hazard_train_df_part2, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "stdr_tm"], target, "apt_hazard_mean")

        # 그리드별-요일별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
        hazard_train_df_part3, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "day_nm"], target, "apt_hazard_mean")

        # 그리드별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
        hazard_train_df_part4, null_df = merge_statistic(statistic_df, null_df, "grid_id", target, "apt_hazard_mean")

        # 행정동코드별 (시간별 위해지표 평균, 요일별 위해지표 평균, 습도별 위해지표 평균, 기온별 위해지표 평균, 행렬번호별 위해지표 평균)들의 평균
        hazard_train_df_part5, _ = merge_statistic(statistic_df, null_df, "admd_cd", target, "apt_hazard_mean")

        hazard_train_df = pd.concat([hazard_train_df_part1, hazard_train_df_part2, hazard_train_df_part3, hazard_train_df_part4, hazard_train_df_part5], axis=0)
        hazard_train_df.drop(["grid_id", "stdr_tm", "admd_cd", "day_nm"], axis=1, inplace=True)
        hazard_train_df.reset_index(drop=True, inplace=True)

        return hazard_train_df

    def make_weak_train_df(statistic_df: pd.DataFrame, train_df: pd.DataFrame) -> pd.DataFrame:
        """01-(5) : 취약지표(weak_score) 학습 데이터를 생성하는 함수

        Parameters
        ----------
        statistic_df : pandas.DataFrame
        파생 변수 데이터 셋
        train_df : pandas.DataFrame
        모델 학습 데이터 셋

        Returns
        -------
        weak_train_df : pandas.DataFrame
        취약지표 모델 학습시킬 데이터

        Notes
        -----
        랜덤샘플링으로 인해 grid, stdr_tm, day_nm 조건을 전부 충족하는 데이터가 없는 경우
        grid_id/stdr_tm/day_nm -> grid_id/tm -> grid_id/day_nm -> grid_id -> admd_cd 순으로 N/A값을 채운다
        """

        statistic_df = statistic_df[["grid_id", "stdr_tm", "admd_cd", "day_nm", "tm_weak_mean", "day_weak_mean", "grid_weak_mean", "grid_weak_max"]]
        train_df = train_df[["grid_id", "stdr_tm", "admd_cd", "day_nm", "weak_score"]]

        # 생성할 파생변수 리스트
        target = ["tm_weak_mean", "day_weak_mean", "grid_weak_mean", "grid_weak_max"]

        # 그리드별-시간별-요일별 ( 시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값 )들의 평균
        weak_train_df_part1, null_df = merge_statistic(statistic_df, train_df, ["grid_id", "stdr_tm", "day_nm"], target, "tm_weak_mean")

        # 그리드별-시간별 ( 시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값 )들의 평균
        weak_train_df_part2, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "stdr_tm"], target, "tm_weak_mean")

        # 그리드별-요일별 ( 시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값 )들의 평균
        weak_train_df_part3, null_df = merge_statistic(statistic_df, null_df, ["grid_id", "day_nm"], target, "tm_weak_mean")

        # 그리드별 ( 시간별 취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값 )들의 평균
        weak_train_df_part4, null_df = merge_statistic(statistic_df, null_df, "grid_id", target, "tm_weak_mean")

        # 행정동코드별 ( 시간별-취약지표 평균, 요일별 취약지표 평균, 행렬번호별 취약지표 평균값, 행렬번호별 취약지표 최댓값 )들의 평균
        weak_train_df_part5, null_df = merge_statistic(statistic_df, null_df, "admd_cd", target, "tm_weak_mean")

        weak_train_df = pd.concat([weak_train_df_part1, weak_train_df_part2, weak_train_df_part3, weak_train_df_part4, weak_train_df_part5], axis=0)
        weak_train_df.drop(["grid_id", "stdr_tm", "admd_cd", "day_nm"], axis=1, inplace=True)
        weak_train_df.reset_index(drop=True, inplace=True)

        return weak_train_df

    # 01-(1) data_preprocessing.py 에서 전처리된 데이터 불러오기
    preprocessed_df = import_preprocessed()

    # 01-(2) 기초 파생변수 생성
    preprocessed_df = make_basic_var(preprocessed_df)

    # 01-(3) 중간 날짜로 데이터 분리
    statistic_df, train_df = split_df_by_date(preprocessed_df)

    # 01-(4) 위해지표 학습 데이터 생성
    hazard_train_df = make_hazard_train_df(statistic_df, train_df)

    # 01-(5) 취약지표 학습 데이터 생성
    weak_train_df = make_weak_train_df(statistic_df, train_df)

    return hazard_train_df, weak_train_df


def train_hazard_model(hazard_train_df: pd.DataFrame):
    """02. hazard (위해지표) 예측 모델 생성

    Parameters
    ----------
    hazard_train_df: pandas.DataFrame
       앞 변수생성 전처리가 끝난 merge 데이터

    Notes
    -----
    4개 Fold의 개별 폴더를 매개변수로 모델링 함수에 전달, 모델링 함수는 반복문을 돌며 로그와 모델을 전역변수에 저장
    모델 생성 후  회귀계수 값을 비교하며 최적 모델 선정, 모델 파일 저장
    """

    X = hazard_train_df[["tm_hazard_mean", "day_hazard_mean", "hd_hazard_mean", "apt_hazard_mean", "grid_hazard_mean"]].values
    y = np.log(hazard_train_df["hazard_score"] + 1)

    # seed 및 cv 생성
    ss = ShuffleSplit(n_splits=MODEL_FOLD_CNT, test_size=0.25, random_state=2023)
    
    # 각 모델을 만드는 함수 실행
    for fold_num, (train_idx, test_idx) in enumerate(ss.split(X)):

        # 학습데이터, 테스트데이터 분리
        train_X, train_y = X[train_idx], y[train_idx]
        predict_X, predict_y = X[test_idx], y[test_idx]
        
        data_set = train_X, train_y, predict_X, predict_y, fold_num

        XGBoost(*data_set).training()
        LGBM(*data_set).training()
    
    else:
        # 최적성능 모델 선택 & 저장 함수 실행
        ModelFactory.select_best_model("hazard", gu_nm)
        
def train_weak_model(weak_train_df: pd.DataFrame):
    """03. weak(취약지표) 예측 모델을 생성하고 평가하는 함수

    Parameters
    ----------
    weak_train_df: pandas.DataFrame
       앞 변수생성 전처리가 끝난 merge 데이터

    Notes
    -----
    4개 Fold의 개별 폴더를 매개변수로 모델링 함수에 전달, 모델링 함수는 반복문을 돌며 로그와 모델을 전역변수에 저장
    모델 생성 후  회귀계수 값을 비교하며 최적 모델 선정, 모델 파일 저장
    """

    X = weak_train_df[["tm_weak_mean", "day_weak_mean", "grid_weak_mean", "grid_weak_max"]].values
    y = weak_train_df.weak_score

    # seed 및 cv 생성
    ss = ShuffleSplit(n_splits=MODEL_FOLD_CNT, test_size=0.25, random_state=2023)

    # 각 모델을 만드는 함수 실행
    for fold_num, (train_idx, test_idx) in enumerate(ss.split(X)):

        train_X, train_y = X[train_idx], y[train_idx]
        predict_X, predict_y = X[test_idx], y[test_idx]

        # 초기화 인스턴스 변수들을 패킹
        data_set = train_X, train_y, predict_X, predict_y, fold_num

        XGBoost(*data_set).training()
        LGBM(*data_set).training()
    else:
        # 최적성능 모델 선택 & 저장 함수 실행
        ModelFactory.select_best_model("weak", gu_nm)

def safe_idex_model_learning_main(arg_date: str, arg_gu_nm: str):
    
    global gu_nm

    # logging setting
    format="%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    logging.info(f"Main-Process : Start")
    start = time.time()

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
    _ = arg_date
    gu_nm = arg_gu_nm

    # 01. 학습 데이터 생성 함수
    hazard_train_df, weak_train_df = create_train_df()
    logging.info("-- create_train_df() 종료 --")
    
    # 02. 위해지표 모델 학습
    train_hazard_model(hazard_train_df)
    logging.info("-- train_hazard_model() 종료 --")   
    del hazard_train_df
    
    # 03. 취약지표 모델 학습
    train_weak_model(weak_train_df)
    logging.info("-- train_weak_model() 종료 --")
    del weak_train_df
    
    logging.info(f"{gu_nm} End --")

    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split(".")[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")
