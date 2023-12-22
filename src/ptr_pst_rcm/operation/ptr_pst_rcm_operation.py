#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal
import logging
import time
from typing import (
    Type,
    Tuple,
)

from dateutil.relativedelta import relativedelta
import geopandas as gpd
from haversine import haversine
import numpy as np
import pandas as pd
from pyspark.sql.utils import ParseException
from py4j.protocol import Py4JJavaError
from shapely.geometry import Point
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from soss.ptr_pst_rcm.operation.log_object import LogObject
from soss.spark_session.db_connector import SparkClass
from soss.queries.ptr_pst_rcm_query import PtrPstRcmQuery
from soss.utils.config_parser import *

MONTH_PERIOD = 2

def delete_soss_ptr_pst_rcm(today: str, gu_nm: str, gu_cd: int):

    # define delete date
    delete_date = datetime.strptime(today, "%Y%m%d")
    delete_date = (delete_date - relativedelta(days=2))
    delete_date = datetime.strftime(delete_date, "%Y%m%d")

    # Create an engine to connect to the database
    engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}")

    # 세션을 사용하여 데이터베이스 작업 수행
    with Session(engine) as session:
        
        # 순찰거점 테이블
        table = "SOSS.DM_PTR_PST"
        
        try:
            # 데이터 삭제
            session.execute(f"DELETE FROM {table} WHERE stdr_de <= '{delete_date}' AND gu_cd='{gu_cd}'")
            time.sleep(10)
            
            session.execute(f"DELETE FROM {table} WHERE stdr_de = '{today}' AND gu_cd='{gu_cd}'")
            time.sleep(10)

            # 정상 수행시 commit
            session.commit()
            logging.info(f"DatMart {table} {delete_date} 이전 일자 Delete!")
            logging.info(f"DatMart {table} {today} 일자 {gu_nm} Delete!")            
        except:
            # 예외가 발생한 경우 롤백
            session.rollback()
            logging.error(f"Error raisd. Transaction Rollback!")

    engine.dispose()

def create_data_mart(today_dt) -> pd.DataFrame:
    """
    안전도 데이터에 그리드 좌표, 지구대 데이터를 결합하여 통합 데이터마트 구축하는 함수

    Returns
    -------
    merge_df : pandas.DataFrame
       input 데이터 프레임에 그리드 좌표 정보와 지구대 정보를 결합한 데이터 프레임

    Notes
    -----
    safe_df에 그리드 좌표 정보 테이블과 행정동 지구대 매핑 테이블을 left_join으로 순차적으로 결합시켜 통합 데이터 마트를 생성
    """

    # 1. 안전지수 그리드 데이터 로딩
    try:
        safe_df = query_obj.get_dm_safe_idex_grid_sql()
        if not safe_df.take(1):
            raise Exception("Fucntion : get_dm_safe_idex_grid_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        safe_df = safe_df.toPandas()
        safe_df["ptr_tmzn_cd"] = safe_df["stdr_tm"] // 4
        safe_df = safe_df.groupby(by=["grid_id", "ptr_tmzn_cd"], as_index=False)["safe_idex"].mean()

    # 2. 그리드 좌표 데이터 로딩
    try:
        pcell_info = query_obj.get_pcel_stdr_info_sql()
        if not pcell_info.take(1):
            raise Exception("Fucntion : get_pcel_stdr_info_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        pcell_info = pcell_info.toPandas()

        # 구 안전도 데이터와 그리드 좌표(pcell_info) 결합하여 데이터프레임 생성
        merge_df = safe_df.merge(pcell_info, how="left", on="grid_id")

    # 3. 같은 일자기준 최근 유동인구 정보가 있는 그리드만 추출
    try:
        for year in range(0, 5):
            # 일자 설정
            start_date = (today_dt - relativedelta(years=year, months=MONTH_PERIOD)).strftime("%Y%m%d")
            end_date = (today_dt - relativedelta(years=year, days=1)).strftime("%Y%m%d")

            # (MONTH_PERIOD 개월 전 ~ 어제) 사이 유동인구 값이 있는 그리드 ID만 추출
            unique_grid = query_obj.get_distinct_grid_id_sql(start_date, end_date)

            if unique_grid.take(1):
                break
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        unique_grid = unique_grid.toPandas()
        unique_grid = unique_grid["grid_id"]
        merge_df = merge_df[merge_df["grid_id"].isin(unique_grid)]

    # 4. 행정동지구대매핑 데이터 로딩
    try:
        admd_plbx_mapn_df: "pyspark.sql.DataFrame" = query_obj.get_admd_plbx_mapn_sql()
        if not admd_plbx_mapn_df.take(1):
            raise Exception("Fucntion : get_admd_plbx_mapn_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        admd_plbx_mapn_df = admd_plbx_mapn_df.toPandas()
        admd_plbx_mapn_df = admd_plbx_mapn_df.astype({"ptr_vhcl_co": "Int16"})

    # 행정동지구대메핑 테이블 결합
    merge_df = merge_df.merge(admd_plbx_mapn_df, how="left", on="admd_cd")

    # ★ 결측값 임시 제거 (나중에 해당 줄은 지워줘도, 결측값이 안나와야 정상 프로세스)★
    merge_df = merge_df[merge_df.ptr_vhcl_co.notnull()]

    return merge_df

def make_safe_grade(merge_df: pd.DataFrame) -> pd.DataFrame:
    """
    안전도 값(0~100)을 순찰거점분석용 안전등급 부여하는 함수

    Parameters
    ----------
    merge_df : pandas.DataFrame
       데이터 결합이 완료된 통합 데이터 프레임

    Returns
    -------
    merge_df : pandas.DataFrame
       merge_df에 safe_grade(안전등급)이 컬럼이 추가된 데이터 프레임
    """

    # 안전도 등급(위험|주의|안전) 구분하기 위한 로직 설정
    merge_df["plbx_tmzn_ranking"] = merge_df.groupby(["plbx_nm","ptr_tmzn_cd"])["safe_idex"].rank(method="min")

    # 지구대별-순찰시간대코드별 그리드 개수
    grouped_df = merge_df.groupby(["plbx_nm", "ptr_tmzn_cd"], as_index=False)["grid_id"].count()
    grouped_df.rename(columns={"grid_id": "plbx_tmzn_grid_cnt"}, inplace=True)

    # 지구대별-순찰시간대코드별 그리드 데이터 병합
    merge_df = merge_df.merge(grouped_df, how="left", on=["plbx_nm", "ptr_tmzn_cd"])

    """ 안전등급 분류 """

    # 주의 : 지구대별-순찰시간대코드별 그리드 개수의 하위 10%
    caution_condition = (merge_df["plbx_tmzn_ranking"] < (0.1 * merge_df["plbx_tmzn_grid_cnt"]))

    # 위험 : (지구대별-순찰시간대코드   별 그리드 개수의 하위 10%) & (안전도 60 미만)
    danger_condition = caution_condition & (merge_df["safe_idex"] < 60)

    merge_df["safe_grade"] = np.where(danger_condition, "위험", np.where(caution_condition, "주의", "안전"))

    # 불필요한 컬럼(plbx_tmzn_grid_cnt) 제거
    merge_df.drop(["plbx_tmzn_ranking", "plbx_tmzn_grid_cnt"] , axis=1, inplace=True)

    return merge_df

def append_weight(dataFrame: pd.DataFrame, weight: int=9) -> pd.DataFrame:
    """
    가중치 비율 계산에 따라 위험(red)지역은 10배 데이터복제(가중치부여) 하는 함수

    Parameters
    ----------
    dataFrame : pandas.DataFrame

    Returns
    -------
    result_df : pandas.DataFrame
    """

    # SAFE_GRADE가 위험인 데이터프레임과 위험이 아닌 데이터프레임 분리
    danger_df = dataFrame[dataFrame["safe_grade"] == "위험"]
    not_danger_df = dataFrame[dataFrame["safe_grade"] != "위험"]

    # SAFE_GRADE가 위험인 데이터프레임 가중치만큼 복제
    danger_df = pd.concat([danger_df] * weight, ignore_index=True)

    # 기존 데이터프레임에 복제 데이터프레임 행 결합
    result_df = pd.concat([danger_df, not_danger_df], ignore_index=True)

    return result_df

def make_log_object(gu_cd: int):

    # 순찰거점을 기록할 로그 데이터
    log_df = pd.DataFrame(
        columns=[
            "plbx_nm", # 지구대명
            "ptr_tmzn_cd", # 순찰시간대코드
            "gu_cd", # 구코드
            "admd_cd", # 행정동코드
            "lo", # 경도
            "la", # 위도
            "grid_id", # 그리드ID
            "avrg_safe_idex", # 평균안전지수
            "ptr_vhcl_co", # 순찰차량수
            "bst_clsr_co", # 최적군집수
            "ptr_pst_cd" # 순찰거점코드
        ]
    )

    log_df = log_df.astype(
        {
            "plbx_nm": str,
            "ptr_tmzn_cd": "Int32",
            "gu_cd": str,
            "admd_cd": str,
            "lo": "float64",
            "la": "float64",
            "grid_id": str,
            "avrg_safe_idex": "float64",
            "ptr_vhcl_co": "Int16",
            "bst_clsr_co": "Int16",
            "ptr_pst_cd": str
        }
    )

    # log_instance 객체 생성
    log_instance = LogObject()
    log_instance.gu_cd = str(gu_cd)

    return log_df, log_instance

def kmeans_modeling(merge_df: pd.DataFrame, log_df: pd.DataFrame, log_instance) -> pd.DataFrame:
    """
    지구대별, 일자별, 시간대별 군집분석 실시 후 시각화파일 & 결과기록(로그)파일을 저장하는 함수

    Parameters
    ----------
    merge_df : pandas.DataFrame
       그리드 좌표, 지구대 정보 결합 후 안전등급 컬럼이 추가된 데이터 프레임

    Returns
    -------
    log_df : pandas.DataFrame
       모든 지구대, 일자, 시간대별 순찰거점 기록이 완료된 데이터프레임
    """

    global all_grid_df

    # 지구대명 반복문 실행
    for plbx_nm in merge_df["plbx_nm"].unique():

        # 지구대 명별(plbx_nm) 쿼리
        plbx_df = merge_df[merge_df["plbx_nm"] == plbx_nm].copy()

        # 지구대별 전체 mtr_no array 생성(이상 순찰거점 점검용)
        all_grid_df = plbx_df.copy()

        log_instance.plbx_nm = plbx_nm
        log_instance.admd_cd = plbx_df["admd_cd"].values[0]

        # 순찰시간대코드별 반복문 실행
        for ptr_tmzn_cd in np.unique(merge_df["ptr_tmzn_cd"]):

            # 순찰 시간대 코드별(ptr_tmzn_cd) 쿼리
            plbx_tmzn_df = plbx_df[plbx_df["ptr_tmzn_cd"] == ptr_tmzn_cd].copy()

            # 값이 없으면 contiunue
            if plbx_tmzn_df.empty:
                continue

            log_instance.ptr_tmzn_cd = ptr_tmzn_cd

            # 지구대별 소유 순찰차량 수
            ptr_vhcl_co = plbx_tmzn_df["ptr_vhcl_co"].values[0]
            log_instance.ptr_vhcl_co = ptr_vhcl_co

            # K-means에 사용할 군집 X좌표, Y좌표
            x_data = plbx_tmzn_df[["cnt_x_crd", "cnt_y_crd"]]

            # 최적 군집 수 탐색하기
            for cluster_cnt in range(2, 6):

                # K-means parameter(군집을 나눌 개수) : 순찰차 수로 설정
                kmeans = KMeans(n_clusters=cluster_cnt, random_state=2023, n_init=10).fit(x_data)

                if cluster_cnt == 2:
                    # 최적 군집 수 초기화
                    bst_clsr_co = cluster_cnt

                    # 최적의 군집 개수 탐색을 위한 K-means 모델링 & 실루엣 계수 값 비교
                    max_silhouette_score = silhouette_score(x_data, kmeans.labels_)
                    plbx_tmzn_df["cluster_idx"] = kmeans.labels_

                   # 순찰거점 좌표 반환
                    centroids = kmeans.cluster_centers_
                else:
                    compare_silhouette_score = silhouette_score(x_data, kmeans.labels_)

                    # 기존 최대 실루엣 계수값과 새로운 실루엣 계수 값을 비교
                    if max_silhouette_score < compare_silhouette_score:

                        # 최적 군집 수 변경
                        bst_clsr_co = cluster_cnt

                        # 최대 실루엣 계수 변경
                        max_silhouette_score = compare_silhouette_score
                        plbx_tmzn_df["cluster_idx"] = kmeans.labels_

                        # 순찰거점 좌표 반환
                        centroids = kmeans.cluster_centers_

            # 최적군집, 순찰차량 로그 출력
            logging.info(f"-- {plbx_nm} - {ptr_tmzn_cd} timezone -- bst_clsr_co {bst_clsr_co} -- ptr_vhcl_co {ptr_vhcl_co}")

            # 최적 군집 수 기록
            log_instance.bst_clsr_co = bst_clsr_co

            # 클러스터 개수를 기반으로 다른 두 가지 군집함수 케이스 실행
            if bst_clsr_co == ptr_vhcl_co:
                log_instance.ptr_pst_cd = '4'
                log_df = patrol_fit(plbx_tmzn_df, centroids, log_df, log_instance)

            elif bst_clsr_co > ptr_vhcl_co:
                log_instance.ptr_pst_cd = '1'
                log_df = cluster_primary(plbx_tmzn_df, centroids, log_df, log_instance)
            else:
                log_instance.ptr_pst_cd = '3'
                log_df = cluster_over(plbx_tmzn_df, centroids, log_df, log_instance)

    return log_df

def patrol_fit(plbx_tmzn_df: pd.DataFrame, centroids: Tuple[float], log_df, log_instance) -> pd.DataFrame:
    """
    PF (Patrol Fit, 최적군집 수와 순찰차 수가 일치하여 그대로 군집 중점을 도출한 거점)

    Parameters
    ----------
    plbx_tmzn_df : pandas.DataFrame
       지구대명, 기준일자, 순찰시간대코드 쿼리 후 위험 가중치가 반영된 데이터 프레임

    centroids : Tuple[float]

    log_df: pandas.DataFrame
    """

    # 순찰차 갯수 만큼 해당 지역을 군집
    for idx, centroid in enumerate(centroids):

        # cluster별 쿼리 (순찰거점은 각 cluster의 중점)
        cluster_df = plbx_tmzn_df[plbx_tmzn_df["cluster_idx"] == idx]

        # 군집 평균 안전도 기재
        log_instance.avrg_safe_idex= round(cluster_df["safe_idex"].mean(), 3)

        # 정상 군집 거점 확인
        log_instance = check_right_position(centroid[0], centroid[1], log_instance)

        # 최종 순찰거점의 정보를 기록
        log_df = pd.concat([log_df, log_instance.get_info_dataframe()], ignore_index=True)

    return log_df

def cluster_primary(plbx_tmzn_df: pd.DataFrame, centroids: Tuple[float], log_df, log_instance) -> pd.DataFrame:
    """
    CP (Cluster Primary, 최적군집 수가 보유한 순찰차 수 보다 많은 경우 우선적으로 중요 거점을 선택하여 거점 도출)

    Parameters
    ----------
    plbx_tmzn_df : pandas.DataFrame
       지구대명, 기준일자, 순찰시간대코드 쿼리 후 위험 가중치가 반영된 데이터 프레임

    """

    # 1. 군집별 평균안전도 비교를 통해 우선 거점 분류

    # 오름차순으로 평균안전도 순서를 매김
    cluster_ranking = plbx_tmzn_df.groupby("cluster_idx", as_index=False)["safe_idex"].mean()
    cluster_ranking["ranking"] = cluster_ranking["safe_idex"].rank(method="min")

    # 내림차순 순위를 매겼을 때, 순찰차 수 이하의 cluster는 주요 거점 (순위 숫자가 낮을 수록, 평균 안전도 낮음 => 우선 예방)
    primary_idxs = cluster_ranking[cluster_ranking["ranking"] <= log_instance.ptr_vhcl_co]["cluster_idx"].values

    for primary_idx in primary_idxs:

        # cluster별 쿼리 (순찰거점은 각 cluster의 중점)
        cluster_df = plbx_tmzn_df[plbx_tmzn_df["cluster_idx"] == primary_idx]

        # 군집 평균 안전도 기재
        log_instance.avrg_safe_idex = round(cluster_df["safe_idex"].mean(), 3)

        # 정상 군집 거점 확인
        log_instance = check_right_position(centroids[primary_idx][0], centroids[primary_idx][1], log_instance)

        # 최종 순찰거점의 정보를 기록
        log_df = pd.concat([log_df, log_instance.get_info_dataframe()], ignore_index=True)

    return log_df


def cluster_over(plbx_tmzn_df: pd.DataFrame, centroids: Tuple[float], log_df, log_instance) -> pd.DataFrame:
    """
    CO (Cluster Over, 보유한 순찰차 수가 최적군집 수 보다 많은 경우 범위가 가장 넓은 군집에 대하여 한번 더 군집분석을 하여 순찰범위를 줄여 도출한 거점 도출)

    Parameters
    ----------
    plbx_tmzn_df : pandas.DataFrame
       지구대명, 기준일자, 순찰시간대코드 쿼리 후 위험 가중치가 반영된 데이터 프레임

    log_instance : LogObject
       "CO" (Cluster Over) 순찰거점 정보를 기록할 인스턴스
    """

    """ 1. 군집별 그리드 개수 비교를 통해 추가 군집 모델링할 거점 분류 """
    spare_car = log_instance.ptr_vhcl_co - log_instance.bst_clsr_co

    # 내림차순으로 그리드 개수가 많은 클러스터의 순서 번호를 매김 (그리드 개수가 많을수록, 랭킹이 높음)
    cluster_ranking = plbx_tmzn_df.groupby("cluster_idx", as_index=False)["grid_id"].count()
    cluster_ranking["ranking"] = cluster_ranking["grid_id"].rank(ascending=False, method="min")

    no_additional_cluster_idxs = cluster_ranking[cluster_ranking["ranking"] > spare_car]["cluster_idx"].values
    additional_cluster_idx = cluster_ranking[cluster_ranking["ranking"] <= spare_car]["cluster_idx"].values

    """ 2. 군집분석을 추가로 하지 않는 군집 로그 기록 """
    for no_additional_cluster_idx in no_additional_cluster_idxs:

        # cluster별 쿼리 (순찰거점은 각 cluster의 중점)
        cluster_df = plbx_tmzn_df[plbx_tmzn_df["cluster_idx"] == no_additional_cluster_idx]

        # 군집 평균 안전도 기재
        log_instance.avrg_safe_idex = round(cluster_df["safe_idex"].mean(), 3)

        # 정상 군집 거점 확인
        log_instance = check_right_position(centroids[no_additional_cluster_idx][0], centroids[no_additional_cluster_idx][1], log_instance)

        # 최종 순찰거점의 정보를 기록
        log_df = pd.concat([log_df, log_instance.get_info_dataframe()], ignore_index=True)


    """ 3. 추가 군집분석 로그 기록 """
    # 군집분석을 추가로 하는 군집 데이터 추출
    additional_cluster_df = plbx_tmzn_df[plbx_tmzn_df["cluster_idx"].isin(additional_cluster_idx)].copy()

    # K-means에 사용할 군집 X좌표, Y좌표
    x_data = additional_cluster_df[["cnt_x_crd", "cnt_y_crd"]]

    # spare_car + len(additional_cluster_idx) : (남는 순찰차 대수 + 추가 군집할 idx개수)개로 해당 클러스터에 군집 시행
    kmeans = KMeans(n_clusters=spare_car+len(additional_cluster_idx), random_state=2023, n_init=10).fit(x_data)

    # 데이터에 군집 클러스터 부여
    additional_cluster_df["cluster_idx"] = kmeans.labels_

    # 순찰거점 좌표 반환
    additional_centroids = kmeans.cluster_centers_

    for idx, centroid in enumerate(additional_centroids):

        # cluster별 쿼리 (순찰거점은 각 cluster의 중점)
        cluster_df = additional_cluster_df[additional_cluster_df["cluster_idx"] == idx]

        # 군집 평균 안전도 기재
        log_instance.avrg_safe_idex = round(cluster_df["safe_idex"].mean(), 3)

        # 정상 군집 거점 확인
        log_instance = check_right_position(centroid[0], centroid[1], log_instance)

        # 최종 순찰거점의 정보를 기록
        log_df = pd.concat([log_df, log_instance.get_info_dataframe()], ignore_index=True)

    return log_df

def check_right_position(x_crd: float, y_crd: float, log_instance):
    """
    해당 거점이 이상거점인지 확인 후 거점이동하는 함수

    Parameters
    ----------
    x_crd : float
       확인하려는 순찰거점의 X좌표

    y_crd : float
       확인하려는 순찰거점의 Y좌표

    all_grid_df : pandas.DataFrame
       지구대별 관리 공간격자 데이터프레임

    Returns
    -------
    log_instance : LogObject
       log를 기록할 정보를 담은 instance
    """

    # 순찰거점좌표에 grid_id 부여하기
    x_id = (Decimal(str(x_crd)) - Decimal(MIN_X_CRD)) // Decimal("50")
    y_id = (Decimal(str(y_crd)) - Decimal(MIN_Y_CRD)) // Decimal("50")

    position = ','.join([str(x_id), str(y_id)])

    # 순찰거점지역의 grid_id가 input data의 grid_id에 있는지 조회 (정상 조건)
    if sum(np.isin(element=all_grid_df["mtr_no"], test_elements=position)) > 0:

        # 순찰거점 이상징후가 없다면 그대로 원래의 순찰거점 정보를 반환
        grid_id = all_grid_df[all_grid_df["mtr_no"] == position]["grid_id"].values[0]
        lo = str(x_crd)
        la = str(y_crd)
    else:
        logging.info("no grid spot detection : move")

        # 이상 거점과 동일 지구대에서 위험과 주의로 판단된 모든 그리드에 대해 최적 거리 계산
        x_ids = all_grid_df["mtr_no"].str.split(pat=",", expand=True).astype(int)[0]
        y_ids = all_grid_df["mtr_no"].str.split(pat=",", expand=True).astype(int)[1]
        all_grid_df["dist"] = np.sqrt((x_id - x_ids) ** 2) + ((y_id - y_ids) ** 2)

        # 최적거리가 가장 짧은 데이터 추출
        shortest_grid_df = all_grid_df[all_grid_df["dist"] == all_grid_df["dist"].min()]

        # 이상 순찰거점의 좌표를 위에서 추출된 데이터의 좌표로 이동
        grid_id = shortest_grid_df["grid_id"].values[0]
        lo = shortest_grid_df["cnt_x_crd"].values[0]
        la = shortest_grid_df["cnt_y_crd"].values[0]

    # 최종적으로 LogObject 인스턴스에 정보를 기입
    log_instance.lo = float(lo)
    log_instance.la = float(la)
    log_instance.grid_id = grid_id

    return log_instance

def transform_utmk_to_w84(df: pd.DataFrame) -> pd.DataFrame:
    """
    좌표 변환 코드 : 좌표계를 WGS84(epsg:4326)으로 변환하는 코드

    Parameters
    ----------
    df : 위도와 경도 컬럼나 있는 데이터프레임
    """

    # make geometry grid center point
    df["geometry"] = df.apply(lambda row: Point(row["lo"], row["la"]), axis=1)

    # transform grid center point to EPSG:4326 (from EPSG:5179 to EPSG:4326)
    df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:5179")
    df.to_crs(epsg=4326, inplace=True)

    # store EPSG:4326 location
    df["lo"] = df["geometry"].astype(str).str[7:-1].str.split(" ").str[0]
    df["la"] = df["geometry"].astype(str).str[7:-1].str.split(" ").str[1]

    # drop geometry
    df.drop("geometry", axis=1, inplace=True)

    df = df.astype({"lo": "float64", "la": "float64"})

    return df

def delete_spot_near_policebox(log_df: pd.DataFrame, boundary: int=500) -> pd.DataFrame:
    """
    지구대 반경 500 미터 내 순찰거점 제거 코드

    Parameters
    ----------
    df : 위도와 경도 컬럼나 있는 데이터프레임
    """

    try:
        plbx_info = query_obj.get_plbx_info_sql()
        if not plbx_info.take(1):
            raise Exception("Fucntion : get_plbx_info_sql() 쿼리로 가져오는 데이터가 없습니다")
    except (ParseException, Py4JJavaError) as e:
        logging.error(str(e))
    else:
        plbx_info = plbx_info.toPandas()

    tmp_df = log_df.merge(plbx_info, how="left", on="plbx_nm")

    # 거리 계산
    tmp_df["dist"] = tmp_df.apply(lambda row: haversine((row["la"], row["lo"]), (row["plbx_la"], row["plbx_lo"]), unit='m'), axis=1)

    log_df.drop(tmp_df[tmp_df.dist <= 500].index, inplace=True)

    return log_df

def ptr_pst_rcm_operation_main(arg_date: str):

    global query_obj

    # logging setting
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    logging.info("Main-Process : start")
    start = time.time()

    logging.info(f"-- spark load --")

    # .py 실행 파라미터로 실행 현재 날짜(YYYYMMDD)가 들어옴
    today = arg_date

    # today를 datetime 타입으로 변환하여 저장하는 변수
    today_dt : Type[datetime] = datetime.strptime(today, "%Y%m%d")
    
    # 충남 시군구 딕셔너리 "gyeryong_si":44250 (계룡시는 순찰 지구대가 논산에 속해있어 스킵)
    gu_dict = {
        "cheongyang_gun": 44790,
        "geumsan_gun": 44710,
        "seocheon_gun": 44770,
        "buyeo_gun": 44760,
        "yesan_gun": 44810,
        "cheonan_si_seobuk_gu": 44133,
        "hongseong_gun": 44800,
        "cheonan_si_dongnam_gu": 44131,
        "boryeong_si": 44180,
        "taean_gun": 44825,
        "nonsan_si": 44230,
        "gongju_si": 44150,
        "seosan_si": 44210,
        "asan_si": 44200,
        "dangjin_si": 44270,
    }


    record_cnt = 0

    # 구별 반복문 시행
    for gu_nm, gu_cd in gu_dict.items():

        logging.info(f"{gu_nm} Start --")

        # ptr_pst_rcm_query 인스턴스 생성
        query_obj = PtrPstRcmQuery(today, gu_cd)

        # 01. 안전지수 데이터, PCELL 기준정보, 행정동지구대매핑 데이터를 결합하여 Data Mart 생성
        merge_df = create_data_mart()
        logging.info(f"-- create_data_mart() is Finished --")
        logging.info(f"merge_df Count : {merge_df.shape[0]:,}")
        logging.info(f"merge_df Null Check\n{merge_df.isnull().sum()}\n")

        # 02. 안전지수의 파생변수 안전등급 생성
        merge_df = make_safe_grade(merge_df)
        logging.info(f"-- make_safe_grade() is Finished --")
        logging.info(f"merge_df Count : {merge_df.shape[0]:,}")
        logging.info(f"merge_df Null Check\n{merge_df.isnull().sum()}\n")

        # 03. 가중치 부여 함수 실행 => 위험, 주의 그리드만 별도 추출 & 위험그리드 가중치 부여
        merge_df = append_weight(merge_df, weight=14)

        merge_df["gu_cd"] = merge_df["admd_cd"].str[0:5]

        # 04. log 저장 객체 생성
        log_df, log_instance = make_log_object(gu_cd)
        logging.info(f"-- make_log_object() is Finished --")
        logging.info(f"log_df Count : {log_df.shape[0]:,}")
        logging.info(f"log_df Null Check\n{log_df.isnull().sum()}\n")

        # 5. KEANS 모델링 함수 동작
        log_df = kmeans_modeling(merge_df, log_df, log_instance)
        logging.info(f"-- kmeans_modeling() is Finished --")
        logging.info(f"log_df Count : {log_df.shape[0]:,}")
        logging.info(f"log_df Null Check\n{log_df.isnull().sum()}\n")

        # 05. 순찰거점 좌표 형식 변환(UTMK -> WGS84)
        log_df = transform_utmk_to_w84(log_df)
        logging.info(f"-- transform_utmk_to_w84() is Finished --")
        logging.info(f"log_df Count : {log_df.shape[0]:,}")
        logging.info(f"log_df Null Check\n{log_df.isnull().sum()}\n")

        # 06. 지구대 반경 500미터 내 순찰거점 제거 함수
        log_df = delete_spot_near_policebox(log_df)
        logging.info(f"-- delete_spot_near_policebox() is Finished --")
        logging.info(f"데이터 수 : {log_df.shape[0]:,}")
        logging.info(f"log_df Null Check\n{log_df.isnull().sum()}\n")

       # 일자 추가
        log_df["stdr_de"] = today

        # pst_sn 코드 추가
        log_df["pst_sn"] = log_df.reset_index().index + 1 + record_cnt

        record_cnt = log_df["pst_sn"].max()

        # 기존 날짜에 데이터가 있다면 삭제
        delete_soss_ptr_pst_rcm(today, gu_nm, gu_cd)
    
        # 06. 데이터 적재하기
        spark_log = SparkClass.spark.createDataFrame(log_df)
        query_obj.insert_ptr_pst_sql(spark_log)

        logging.info(f"{gu_nm} End --\n")

    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split('.')[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")