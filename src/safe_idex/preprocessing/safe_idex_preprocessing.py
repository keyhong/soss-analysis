#!/usr/bin/env python3
# -*- coding: utf-8-*-

"""
안전지수 전처리 모듈
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
import time
from typing import List, Tuple

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.utils import ParseException
from py4j.protocol import Py4JJavaError

from soss.queries.safe_idex_query import SafeIdexQuery
from soss.spark_session.db_connector import SparkClass

PREPROCESSING_DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
MONTH_PERIOD = 2

def create_grid() -> pd.DataFrame:
    """01. 그리드 데이터를 생성하는 함수
    
    Returns
    -------
    grid_dt_tm_df : pandas.DataFrame
       공간격자 데이터 프레임
    """
    
    def import_grid() -> pd.DataFrame:
        """01-(1) : 전체 공간격자 데이터를 불러오는 함수"""
        
        # 남충시 공간격자 데이터 불러오기
        try:
            grid_df = query_obj.get_pcel_stdr_info_sql()
            if not grid_df.take(1):
                logging.error("Fucntion : get_pcel_stdr_info_sql() 쿼리로 가져오는 데이터가 없습니다")
        except ParseException as e:
            logging.error(f"ParseException : {e}")
        except Py4JJavaError as e:
            logging.error(f"Py4JJavaError : {e}")
        else:
            grid_df = grid_df.toPandas()

        # 메모리 사용량 감소를 위해 데이터 타입을 object에서 int로 변경
        grid_df = grid_df.astype({"admd_cd": "UInt32", "row_no": "UInt16", "clm_no": "UInt16"})

        return grid_df

    def create_date(grid_df: pd.DataFrame) -> pd.DataFrame:
        """01-(2) : 그리드 데이터에 일자를 생성하는 함수

        Parameters
        ----------
        grid_df : pandas.DataFrame
           남충시 전체 그리드 데이터에서 최근 MONTH_PERIOD개월 간 유동인구가 포착된 그리드만 분류하여 추출한 데이터

        Returns
        -------
        grid_df : pandas.DataFrame
           그리드에 일자를 생성한 데이터 프레임
        """

        start_date = (today_dt - relativedelta(months=MONTH_PERIOD)).strftime("%Y%m%d")
        end_date = (today_dt - relativedelta(days=1)).strftime("%Y%m%d")

        # 시작날짜와 끝날짜 사이의 모든 일자 리스트 생성
        days_arr = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()

        # 일자별로 공간격자를 생성한 데이터를 담을 합계 데이터프레임
        grid_dt_df = pd.DataFrame()

        # 일자 확장
        for date in days_arr:
            grid_df["stdr_de"] = date
            grid_dt_df = pd.concat([grid_dt_df, grid_df], ignore_index=True)
        
        logging.info(f"일자 확장 후 : {grid_dt_df.shape[0]:,}")

        # 컬럼 재정렬
        grid_dt_df = grid_dt_df[["stdr_de", "grid_id", "mtr_no", "admd_cd", "row_no", "clm_no"]]

        return grid_dt_df

    def create_time(grid_dt_df: pd.DataFrame) -> pd.DataFrame:
        """01-(4) : 그리드 데이터에 시간을 생성하는 함수

        Parameters
        ----------
        grid_dt_df : pandas.DataFrame
           그리드에 일자를 생성한 데이터 프레임

        Returns
        -------
        grid_dt_tm_df : pandas.DataFrame
           그리드에 시간을 생성한 데이터 프레임
        """

        # 시간별로 공간격자를 생성한 데이터를 담을 합계 데이터프레임
        grid_dt_tm_df = pd.DataFrame() 
        
        # 시간 확장
        for tm in range(0, 24):
            grid_dt_df["stdr_tm"] = tm
            grid_dt_tm_df = pd.concat([grid_dt_tm_df, grid_dt_df], ignore_index=True)
        
        logging.info(f"시간 확장 후 : {grid_dt_tm_df.shape[0]:,}")
        
        # type casting
        grid_dt_tm_df["stdr_tm"] = grid_dt_tm_df["stdr_tm"].astype("UInt8")
        
        # 컬럼 재정렬
        grid_dt_tm_df = grid_dt_tm_df[["stdr_de", "stdr_tm", "grid_id", "mtr_no", "admd_cd", "row_no", "clm_no"]]

        return grid_dt_tm_df

    # 01-(1) 그리드 데이터 생성하는 함수
    grid_df = import_grid()

    # 01-(2) 공간격자 데이터에 일자 변수 생성
    grid_dt_df = create_date(grid_df)    
    
    # 01-(3) 공간격자 데이터에 시간 변수 생성
    grid_dt_tm_df = create_time(grid_dt_df)
    
    return grid_dt_tm_df

def merge_report(grid_dt_tm_df: pd.DataFrame) -> pd.DataFrame:
    """02. 경찰 신고 접수 데이터 병합하는 함수
    
    Parameters
    ----------
    grid_dt_tm_df : pandas.DataFrame
       공간격자 데이터 프레임

    Returns
    --------
    report_merge_df : pandas.DataFrame
        경찰 신고 접수 데이터 병합한 데이터프레임
    """
    
    def import_report() -> pd.DataFrame:
        """02-(1) : 경찰신고접수 데이터를 가져오는 함수
        
        Returns
        --------
        report_df : pandas.DataFrame
           경찰청 신고 데이터 프레임  
        """

        try:
            start_month = (today_dt - relativedelta(months=MONTH_PERIOD)).strftime("%m")
            end_month = (today_dt - relativedelta(days=1)).strftime("%m")
            report_df = query_obj.get_npa_dcr_rcp_sql(start_month, end_month)
            
            if not report_df.take(1):
                logging.error("Fucntion : get_npa_dcr_rcp_sql() 쿼리로 가져오는 데이터가 없습니다")
        except ParseException as e:
            logging.error(f"ParseException : {e}")
        except Py4JJavaError as e:
            logging.error(f"Py4JJavaError : {e}")
        else:
            report_df = report_df.toPandas()

        report_df["stdr_de"] = report_df["stdr_de"].str.slice_replace(0, 4, today[0:4])

        # type casting
        report_df = report_df.astype({"stdr_tm": "UInt8"})

        return report_df

    def classify_important_reception(report_df: pd.DataFrame) -> pd.DataFrame:
        """02-(2) : 중범죄 신고를 분별하는 함수
        
        Parameters
        ----------
        report_df : pandas.DataFrame
           경찰청 신고 데이터 프레임

        Returns
        --------
        important_report_df : pandas.DataFrame
           중요 신고 데이터를 분별한 데이터 프레임   
           
        """

        # 중범죄 사건 리스트 
        important_report = [14, 15, 16, 17]

        # 신고접수 시간(stdr_tm)이 important_report안에 있으면 1 없으면 0을 부여하는 컬럼을 생성
        report_df["importance"] = np.where(report_df["stdr_tm"].isin(important_report), np.uint8(1), np.uint8(0))
        
        # "일자별-시간별-행렬번호별" 로 그룹핑하여, 사건 개수와 중요사건 합계 반환
        important_report_df = report_df.groupby(by=["stdr_de", "stdr_tm", "mtr_no"], as_index=False).agg(report_cnt=("mtr_no", "count"), impt_report_cnt=("importance", "sum"))

        # type casting
        important_report_df = important_report_df.astype({"report_cnt": "UInt16", "impt_report_cnt": "UInt16"})

        return important_report_df

    def classify_urgent_reception(report_df: pd.DataFrame) -> pd.DataFrame:
        """02-(3) : 긴급한 신고를 분별하는 함수
        
        Parameters
        ----------
        report_df : pandas.DataFrame
           경찰청 신고 데이터 프레임

        Returns
        --------
        urgent_report_df : pandas.DataFrame
           긴급 신고 데이터를 분별한 데이터 프레임
        """

        # 접수유형이 (C0, C1)인 데이터 => 긴급 신고 데이터만 추출
        urgent_report = ["C0", "C1"]
        report_df = report_df[report_df["incd_emr_cd"].isin(urgent_report)].copy()
        report_df["isUrgent"] = 1

        # "일자-시간-행렬번호-사건종별명" 으로 피벗하여, 접수유형별 개수 반환 (긴급 사건)
        urgent_report_df = report_df.pivot_table(
            index=["stdr_de", "stdr_tm", "mtr_no"],
            columns="incd_emr_cd",
            values="isUrgent",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        # 피벗의 대상열 제거
        urgent_report_df.rename_axis(None, axis=1, inplace=True)

        # type cast 
        urgent_report_df = urgent_report_df.astype({"C0": "UInt16", "C1": "UInt16"})

        return urgent_report_df
    
    def create_hazard_score(impt_urgent_report_df: pd.DataFrame) -> pd.DataFrame:
        """02-(4) : 신고 점수 생성 및 신고 발생 종류(중요/긴급)에 따라 가중치를 부여하는 함수

        Parameters
        ----------
        impt_urgent_report_df : pandas.DataFrame
           분별한 중요신고와 긴급신고를 결합한 데이터프레임

        Returns
        --------
        hazard_df : pandas.DataFrame
           위해지표를 생성한 데이터프레임
        """
        # 위해지표를 생성할 데이터프레임 생성
        hazard_df = impt_urgent_report_df.copy()
        
        # 신고점수(위해지표) 생성
        hazard_df["hazard_score"] =  hazard_df["report_cnt"].copy()

        # 개별 그리드에서 발생한 모든 신고 중에서 중요 신고가 절반이상 일 때 5배 가중치 부여
        hazard_df["hazard_score"] = np.where(hazard_df["impt_report_cnt"] / hazard_df["report_cnt"] > 0.5, hazard_df["hazard_score"] * 5, hazard_df["hazard_score"])

        # 개별 그리드에서 발생한 모든 신고 중에서 긴급 신고(C0, C1)가 있다면 각각 30배, 10배 가중치 부여
        hazard_df["hazard_score"] = np.where(hazard_df["C0"] > 0, hazard_df["hazard_score"] * 30, hazard_df["hazard_score"])
        hazard_df["hazard_score"] = np.where((hazard_df["C0"] == 0) & (hazard_df["C1"] > 0), hazard_df["hazard_score"] * 10, hazard_df["hazard_score"])

        # 중요 사건이면서 긴급신고인 사건에 라벨링 컬럼 추가
        hazard_df["priority"] = np.where((hazard_df["impt_report_cnt"] / hazard_df["report_cnt"] > 0.5) & (hazard_df["C0"] > 0), 1, 0)
        
        # type cast
        hazard_df = hazard_df.astype({"hazard_score": float, "priority": "UInt8"})

        # 컬럼 정리
        hazard_df.drop(["report_cnt", "impt_report_cnt", "C0", "C1"], axis=1, inplace=True)

        return hazard_df

    def expand_time(hazard_df: pd.DataFrame) -> pd.DataFrame:
        """02-(5) : 신고 데이터의 시간을 확장하여, 동일 신고 발생 그리드의 1시간 전후 시간내 가중 점수를 더해주는 함수

        Parameters
        ----------
        hazard_df : pandas.DataFrame
           위해지표를 생성한 데이터프레임

        Returns
        --------
        merge_df : pandas.DataFrame
           그리드 데이터와 경찰청 신고 데이터를 병합한 데이터 프레임
        """

        # (1) 우선 사건 라벨링 테이블 추출 (report 데이터 grouping시, 라벨 컬럼은 삭제되기 때문에 이후 다시 재결합)
        priority_df = hazard_df[hazard_df["priority"]==1].copy()
        priority_df = priority_df[["stdr_de", "stdr_tm", "mtr_no", "priority"]]

        # (2) 신고 데이터 시간 확장 : (-1 시간 ~ 현재 ~ +1시간)의 데이터에 가중 신고점수 부여
        report_expand_df = pd.DataFrame()

        for tm in range(-1, 2):

            hazard_copy_df = hazard_df.copy()

            hazard_copy_df["stdr_tm"] = hazard_copy_df["stdr_tm"] + tm
            hazard_copy_df["hazard_score"] = hazard_copy_df["hazard_score"] * (1 - (0.5 * abs(tm)))

            report_expand_df = pd.concat([report_expand_df, hazard_copy_df], ignore_index=True)
            
        # (3) 추가해준 데이터에 대한 일자, 시간 재조정 (시간을 더해주거나 감소시켜, 일자가 바뀌는 경우)
        
        # 날짜 형식으로 데이터 타입 변환
        report_expand_df["stdr_de"] = report_expand_df["stdr_de"].astype("datetime64[ns]") 

        # TM이 0보다 작으면, 일자는 하루 감소, 시간은 24 증가
        report_expand_df["stdr_de"] = np.where(report_expand_df["stdr_tm"] < 0, report_expand_df["stdr_de"] - timedelta(days=1), report_expand_df["stdr_de"])
        report_expand_df["stdr_tm"] = np.where(report_expand_df["stdr_tm"] < 0, report_expand_df["stdr_tm"] + 24, report_expand_df["stdr_tm"])

        # TM이 23보다 크면, 일자는 하루 증가, 시간은 24 감소
        report_expand_df["stdr_de"] = np.where(report_expand_df["stdr_tm"] > 23, report_expand_df["stdr_de"] + timedelta(days=1), report_expand_df["stdr_de"])
        report_expand_df["stdr_tm"] = np.where(report_expand_df["stdr_tm"] > 23, report_expand_df["stdr_tm"] - 24, report_expand_df["stdr_tm"])

        # type casting
        report_expand_df["stdr_de"] = report_expand_df["stdr_de"].apply(lambda x: datetime.strftime(x, "%Y%m%d"))
        report_expand_df["stdr_tm"] = report_expand_df["stdr_tm"].astype("UInt8")

        # (4) 신고점수 합계
        report_expand_df = report_expand_df.groupby(by=["stdr_de", "stdr_tm", "mtr_no"], as_index=False).agg(hazard_score=("hazard_score", "sum"))

        # (5) 시작일과 종료일을 벗어나는 데이터 제거
        start_date = (today_dt - relativedelta(months=MONTH_PERIOD)).strftime("%Y%m%d")
        end_date = (today_dt - relativedelta(days=1)).strftime("%Y%m%d")

        condition = (start_date <= report_expand_df["stdr_de"]) & (report_expand_df["stdr_de"] <= end_date)
        report_expand_df = report_expand_df[condition]

        # (6) 우선사건 라벨링 결합
        report_expand_df = report_expand_df.merge(priority_df, how="left", on=["stdr_de", "stdr_tm", "mtr_no"])
        report_expand_df["priority"].fillna(1, inplace=True)

        return report_expand_df

    # 02-(1) 신고 데이터를 불러오는 함수
    report_df = import_report()        
        
    # 02-(2) 중범죄 신고를 분별하는 함수
    important_report_df = classify_important_reception(report_df)

    # 02-(3) 긴급한 신고를 분별하는 함수
    urgent_report_df = classify_urgent_reception(report_df)

    # 중범죄 신고, 긴급신고 테이블 결합
    impt_urgent_report_df = important_report_df.merge(urgent_report_df, how="outer", on=["stdr_de", "stdr_tm", "mtr_no"])
    impt_urgent_report_df.fillna(value={"C0": 0, "C1": 0}, inplace=True)

    # 02-(4) 위해지표 생성 및 신고 발생 종류(중요/긴급)에 따라 가중점수를 부여하는 함수
    hazard_df = create_hazard_score(impt_urgent_report_df)

    # 02-(5) 1시간 전후의 시간에 따라 신고 점수를 확장하는 함수
    report_expand_df = expand_time(hazard_df)

    # 02-(6) 공간격자 데이터와 신고 데이터 병합
    report_merge_df = grid_dt_tm_df.merge(report_expand_df, how="left", on=["stdr_de", "stdr_tm", "mtr_no"])
    report_merge_df.fillna(value={"hazard_score": 0, "priority": 0}, inplace=True)
    
    return report_merge_df

def expand_grid_scrore(report_merge_df: pd.DataFrame) -> pd.DataFrame:
    """03. 핵심 신고 위치 주변으로 핵심 신고 발생 그리드의 가중치를 덧셈 연산해주는 함수
    
    Parameters
    ----------
    report_merge_df : pandas.DataFrame
        경찰 신고 접수 데이터 병합한 데이터프레임

    Returns
    --------
    grid_expand_df : pandas.DataFrame
        공간격자 확장한 데이터프레임
    """
    
    def expand_grid(priority_df: pd.DataFrame, weight: float=0.3, expand_cnt: int=2) -> pd.DataFrame:
        """03-(1) : 신고 발생 데이터를 주변 그리드로 공간 확장하는 함수

        Parameters
        ----------
        priority_df : pandas.DataFrame
           공간 확장 시키려고 하는 데이터프레임
        weight : float
           공간 확장시마다 신고점수에 곱하는 가중치 점수의 감소분
        expand_cnt : int
           상하 좌우 및 대각선으로 확장하려는 공간격자의 범위 수

        Returns
        -------
        priority_expand_df : pandas.DataFrame
            우선 순위 공간격자를 확장한 데이터프레임
        """

        priority_expand_df = pd.DataFrame()

        for _, row in priority_df.iterrows():

            # 1칸 ~ 상하 좌우 및 대각선 총 expand_cnt칸 확장 개수만큼 돌면서 주변 격자의 번호 조회
            for expand_num in range(1, expand_cnt+1): 

                priority_expand_part_df = pd.DataFrame(
                    print_mtr_no(row["row_no"], row["clm_no"], expand_num),
                    columns=["mtr_no"]
                )

                priority_expand_part_df["stdr_de"] = row["stdr_de"]
                priority_expand_part_df["stdr_tm"] = row["stdr_tm"]
                priority_expand_part_df["expand_hazard_score"] = row["hazard_score"] * np.float64(1 - (expand_num * weight))

                priority_expand_df = pd.concat([priority_expand_df, priority_expand_part_df], ignore_index=True)

        priority_expand_df = priority_expand_df.groupby(["stdr_de", "stdr_tm", "mtr_no"], as_index=False)["expand_hazard_score"].sum()

        return priority_expand_df

    def print_mtr_no(row_no: int, clm_no: int, expand_num: int) -> "numpy.ndarray":
        """03-(2) : 확장하려는 공간격자의 행렬 번호를 반환하는 함수

        Parameters
        ----------
        row_no : int
           공간격자의 행 번호
        clm_no : int
           공간격자의 열 번호
        expand_cnt : int
           확장 하려는 공간의 범위 수

        Returns
        -------
        target_grid_arr : numpy.ndarray
           인풋한 행번호와 열번호를 기준으로 distnace만큼 확장한 그리드들의 행렬번호를 모아둔 어레이
        """

        dxy = [ row_no-expand_num, row_no+expand_num, clm_no-expand_num, clm_no+expand_num ]

        row_range = [ row for row in range(row_no - expand_num, row_no + expand_num+1) ]
        clm_range = [ clm for clm in range(clm_no - expand_num, clm_no + expand_num+1) ]

        grid_list1 = [",".join([str(x_id), str(y)]) for x_id in dxy[0:2] for y in clm_range]
        grid_list2 = [",".join([str(x), str(y_id)]) for y_id in dxy[3:5] for x in row_range]

        target_grid_arr = np.unique(np.array(grid_list1 + grid_list2))

        return target_grid_arr

    # 공간확장 대상 : 신고가 발생하고, 최우선(중요사건 & 긴급신고) 조건을 충족하는 데이터만 추출
    priority_df = report_merge_df[report_merge_df["priority"]==1].copy()
    priority_df = priority_df[["stdr_de", "stdr_tm", "row_no", "clm_no", "hazard_score"]]

    # 공간을 확장하여 신고점수를 산출한 데이터프레임
    priority_expand_df = expand_grid(priority_df, weight=0.3, expand_cnt=2)
    grid_expand_df = report_merge_df.merge(priority_expand_df, how="left", on=["stdr_de", "stdr_tm", "mtr_no"])
    
    # 테이블 병합 후 expand_hazard_score 결측값 채우기
    grid_expand_df["expand_hazard_score"].fillna(0, inplace=True)
    
    # 위해지표 합계 계산
    grid_expand_df["hazard_score"] = grid_expand_df["hazard_score"] + grid_expand_df["expand_hazard_score"]

    # 불필요 컬럼 제거
    grid_expand_df.drop(["mtr_no", "row_no", "clm_no", "priority", "expand_hazard_score"], axis=1, inplace=True)
    
    return grid_expand_df
    
def merge_weather(grid_expand_df: pd.DataFrame) -> pd.DataFrame:
    """04. 날씨 데이터를 병합하는 함수

    Parameters
    -----------
    grid_expand_df : pandas.DataFrame
       날씨 데이터를 병합할 데이터 프레임

    Returns
    -------
    tmp_wether_merge_df : pandas.DataFrame
       날씨 데이터를 병합한 데이터 프레임
    
    Notes
    -----
    실시간 날씨 데이터를 병합하고, 결측값은 지난년도의 날씨 데이터를 병합한다. 그래도 결측값이 있다면 같은 격자, 같은 시간, 다른 날짜의 선형 값으로 보간
    
    """   
    
    def import_wether() -> pd.DataFrame:
        """04-(1) : 일배치 날씨 데이터를 로딩하는 함수

        Returns
        -------
        weather_df : pandas.DataFrame
           일배치의 날씨 데이터
        """

        # 2개월 전 ~ 어제(매월 1일 기준)의 날씨 데이터 로딩
        try:
            start_date = (today_dt - relativedelta(months=MONTH_PERIOD)).strftime("%Y%m%d")
            end_date = (today_dt - relativedelta(days=1)).strftime("%Y%m%d")

            weather_df = query_obj.get_wth_frcs_sql(start_date, end_date)
            
        except ParseException as e:
            logging.error(f"ParseException : {e}")
        except Py4JJavaError as e:
            logging.error(f"Py4JJavaError : {e}")
        else:
            weather_df = weather_df.toPandas()
        
        weather_df = weather_df.astype({"stdr_tm": "UInt8", "admd_cd": "UInt32"})

        return weather_df

    def import_tmp_weather() -> pd.DataFrame:
        """04-(2) : 보간용 날씨 데이터를 로딩하는 함수

        Returns
        -------
        tmp_weather_df : pandas.DataFrame
           보간용 날씨 데이터
        """

        try:
            tmp_weather_df = query_obj.get_tmp_wth_frcs_sql()
            if not tmp_weather_df.take(1):
                logging.error("Fucntion : get_tmp_wth_frcs_sql() 쿼리로 가져오는 데이터가 없습니다")
        except ParseException as e:
            logging.error(f"ParseException : {e}")
        except Py4JJavaError as e:
            logging.error(f"Py4JJavaError : {e}")
        else:
            tmp_weather_df = tmp_weather_df.toPandas()
            tmp_weather_df["stdr_de"] = tmp_weather_df["stdr_de"].str.slice_replace(0, 4, today[0:4])
            
            tmp_weather_df = tmp_weather_df.astype({"stdr_tm": "UInt8", "admd_cd": "UInt32"})
            tmp_weather_df.rename(columns={"hd_val": "sub_hd_val", "atp_val": "sub_atp_val"}, inplace=True)

        return tmp_weather_df

    # 날씨 데이터 불러와서 결합하기
    weather_df = import_wether()
    weather_merge_df = grid_expand_df.merge(weather_df, how="left", on=["stdr_de", "stdr_tm", "admd_cd"])

    # 결합된 날씨 데이터에 N/A값이 있다면, 2022년 데이터를 활용해 빈 데이터 보간
    tmp_wether_df = import_tmp_weather()
    tmp_wether_merge_df = weather_merge_df.merge(tmp_wether_df, how="left", on=["stdr_de", "stdr_tm", "admd_cd"])

    tmp_wether_merge_df["atp_val"] = np.where(tmp_wether_merge_df["atp_val"].isnull(), tmp_wether_merge_df["sub_atp_val"], tmp_wether_merge_df["atp_val"])
    tmp_wether_merge_df["hd_val"] = np.where(tmp_wether_merge_df["hd_val"].isnull(), tmp_wether_merge_df["sub_hd_val"], tmp_wether_merge_df["hd_val"])
    tmp_wether_merge_df.drop(["sub_hd_val", "sub_atp_val"], axis=1, inplace=True)

    # 대체 데이터를 결합해도 N/A 값이 있다면 정렳후 결측값을 선형보간법으로 채우기
    tmp_wether_merge_df.sort_values(by=["admd_cd", "stdr_tm", "stdr_de"], inplace=True)
    tmp_wether_merge_df["atp_val"] = tmp_wether_merge_df["atp_val"].interpolate()
    tmp_wether_merge_df["hd_val"] = tmp_wether_merge_df["hd_val"].interpolate()
    
    return tmp_wether_merge_df
    

def merge_pop(wether_merge_df: pd.DataFrame) -> pd.DataFrame:
    """05. 유동인구 통계 데이터를 병합하는 함수

    Parameters
    -----------
    wether_merge_df : pandas.DataFrame
       유동인구 통계 데이터를 병합할 데이터 프레임

    Returns
    -------
    pop_merge_df : pandas.DataFrame
       유동인구 통계 데이터를 병합한 데이터 프레임
    """    

    def import_pop() -> pd.DataFrame:
        """ 05-(1) 유동인구 통계 데이터 불러오기
         
        Returns
        -------
        pop_df : pandas.DataFrame
            유동인구 데이터프레임
        """
        try:

            start_month = (today_dt - relativedelta(months=MONTH_PERIOD)).strftime("%m")
            end_month = (today_dt - relativedelta(days=1)).strftime("%m")

            months: Tuple[str, str] = (start_month, end_month)

            sdf_lst: List["pyspark.sql.DataFrame"] = []

            for month in months:
                pop_df_part = query_obj.get_dw_pcell_tmzn_fpop_sql(month)

                # 통계용 데이터 요일 컬럼 생성
                pop_df_part = pop_df_part.withColumn("day_nm", F.to_date(F.unix_timestamp("stdr_de", "yyyyMMdd").cast("timestamp")))
                pop_df_part = pop_df_part.withColumn("day_nm", F.date_format("day_nm", "E"))

                from itertools import chain
                day_dict = { "Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6 }
                mapping = F.create_map([F.lit(x) for x in chain(*day_dict.items())])                
                pop_df_part = pop_df_part.withColumn("day_nm", mapping[pop_df_part["day_nm"]])

                sdf_lst.append(pop_df_part)
        except ParseException as e:
            logging.error(f"ParseException : {e}")
        except Py4JJavaError as e:
            logging.error(f"Py4JJavaError : {e}")
        else:
            # 두 테이블 세로병합
            pop_df = sdf_lst[0].unionAll(sdf_lst[1])
            
            # 그룹핑핑 값으로 평균 취약지표 산출
            pop_df = pop_df.groupby(["grid_id", "stdr_tm", "day_nm"]).agg(F.mean("weak_score").alias("weak_score"))                
            
            pop_df = pop_df.toPandas()
            pop_df["stdr_tm"] = pop_df["stdr_tm"].astype("UInt8")

            return pop_df
    
    # 유동인구 데이터 로딩
    pop_df = import_pop()

    # wether_merge_df에 요일 컬럼 추가
    wether_merge_df["day_nm"] = pd.to_datetime(wether_merge_df["stdr_de"]).dt.weekday
    pop_merge_df = wether_merge_df.merge(pop_df, how="left", on=["grid_id", "stdr_tm", "day_nm"])
    
    # 테이블 결합 후 weak_score 결측값 채우기
    pop_merge_df["weak_score"].fillna(0, inplace=True)

    return pop_merge_df


def down_sampling(pop_merge_df: pd.DataFrame, target_col: str, nrows: int=1_000_000, target_percent: float=0.25) -> pd.DataFrame:
    """06. 위해지표 데이터를 다운 샘플링하여 학습 데이터의 밸런스을 맞춰주는 함수

    Parameters
    -----------
    pop_merge_df : pandas.DataFrame
       언더샘플링을 적용할 유동인구 병합 데이터프레임
    target_col : str
       언더샘플링의 대상이 기준 대상이 되는 컬럼의 이름
    nrows
       언더샘플링하여 최종적으로 추출하려는 행의 개수
    target_percent: float
       전체 행의 개수에서 맞추려고 하는 신고 발생 데이터의 비율

    Returns
    -------
    sampling_df: pandas.DataFrame
       언더샘플링이 적용되어 추출된 데이터 프레임. nrows개 중 target_percent만큼 신고 데이터가 들어있음

    Notes
    -----
    언더샘플링을 시행시 신고가 발생하지 않았던 데이터가 너무 적어 target_percent 만큼 비율을 채우지 못할 경우, 최대한의 신고 데이터를 모두 사용할 수 있게 제공한다
    """

    random_seed=2023

    target_cnt = int(round(nrows*target_percent)) # 위해 지표 0이 아닌 데이터의 목표 개수
    report_cnt = pop_merge_df.loc[pop_merge_df[target_col] > 0].shape[0] # 위해 지표가 0이 아닌 데이터의 실제 개수
    
    logging.info(f"신고 건수 : {report_cnt:,}")
                 
    if report_cnt < target_cnt:
        logging.info(f"다운 샘플링 시 위해지표가 0이 아닌 데이터가 {target_cnt:,}를 충족하지 못하는 경우")
        n1 = report_cnt
        n2 = int(((1-target_percent) / target_percent) * report_cnt)
        
    else:
        logging.info(f"다운 샘플링 위해지표가 0이 아닌 데이터가 목표 개수 {target_cnt:,}를 충족하는 경우")
        n1 = int(nrows * target_percent)
        n2 = int(nrows * (1 - target_percent))

    sample1 = pop_merge_df.loc[pop_merge_df[target_col] > 0].sample(n=n1, random_state=random_seed) # 신고점수가 0이 아닌 데이터의 비율
    sample2 = pop_merge_df.loc[pop_merge_df[target_col] == 0].sample(n=n2, random_state=random_seed) # 신고점수가 0인 데이터의 비율

    sampling_df = pd.concat([sample1, sample2], ignore_index=True)
    
    return sampling_df

def safe_idex_preprocessing_main(arg_date: str, arg_gu_nm: str):

    global query_obj, today, today_dt

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
    
    # safe_idex_query 인스턴스 생성
    query_obj = SafeIdexQuery(today, gu_cd)

    # 01. 남충 공간격자 데이터 로딩
    grid_dt_tm_df = create_grid()
    logging.info(f"-- create_grid() 종료 -- 데이터 수 : {grid_dt_tm_df.shape[0]:,} 건")
    
    # 02. 경찰 신고 접수 데이터 병합
    report_merge_df = merge_report(grid_dt_tm_df)
    logging.info(f"-- merge_report() 종료 -- 데이터 수 : {report_merge_df.shape[0]:,} 건")
    del grid_dt_tm_df
    SparkClass.spark.catalog.clearCache()

    # 03. 핵심 신고 위치 주변으로 핵심 신고 발생 그리드의 가중치 곱 점수 덧셈 연산
    grid_expand_df = expand_grid_scrore(report_merge_df)
    logging.info(f"-- expand_grid_scrore() 종료 -- 데이터 수 : {grid_expand_df.shape[0]:,} 건")
    del report_merge_df

    # 04. 날씨 예보 데이터를 병합
    wether_merge_df = merge_weather(grid_expand_df)
    logging.info(f"-- merge_weather() 종료 -- 데이터 수 : {wether_merge_df.shape[0]:,} 건")
    del grid_expand_df
    
    # 05. 유동인구 통계 데이터를 병합
    pop_merge_df = merge_pop(wether_merge_df)
    logging.info(f"-- merge_pop() 종료 -- 데이터 수 : {pop_merge_df.shape[0]:,} 건")
    del wether_merge_df

    # 06. 클래스 불균형을 해소하기 위해 신고 발생 · 미발생 데이터를 일정 비율로 조절(위해지표으로만)
    sampling_df = down_sampling(pop_merge_df, "hazard_score", nrows=1_000_000, target_percent=0.25)
    logging.info("-- down_sampling() 종료 --")
    del pop_merge_df
    
    sampling_df.to_csv(os.path.join(PREPROCESSING_DATA_FOLDER, f"{gu_nm}.csv"), index=False)
    del sampling_df
    
    SparkClass.spark.catalog.clearCache()
    logging.info(f"{gu_nm} End --")
    
    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split('.')[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")    
