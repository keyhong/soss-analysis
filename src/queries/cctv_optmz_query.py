# -*- coding: utf-8-*-

from soss.spark_session.db_connector import SparkClass
from soss.utils.config_parser import *

__all__ = ["CctvOptmzQuery"]

class CctvOptmzQuery:
    """ DW에서 SELECT하고, DM에 INSERT하는 쿼리 모음 클래스 """

    def __init__(self, today: str, gu_cd: str):
        self.today = today
        self.gu_cd = gu_cd

    def get_train_women_pop_sql(self, start_date, end_date) -> "pyspark.sql.DataFrame":
        """ PCELL시간대 성별연령대유형통계 학습데이터를 가져오는 쿼리 반환 함수

        [Column]
        stdr_de   : 기준 일자
        stdr_tm   : 기준 시간
        grid_id   : 그리드ID
        women_pop : PCELL시간대여자수
        """

        sql = f"""
        SELECT /* PCELL시간대여성연령인구합계 */
               stdr_de 
             , stdr_tm
             , grid_id
             , CAST(pcel_tmzn_fml_co AS DOUBLE PRECISION) AS women_pop
          FROM SOSS.DW_PCEL_TMZN_FML_POP_SUM
         WHERE stdr_de BETWEEN '{start_date}' AND '{end_date}'
           AND gu_cd = '{self.gu_cd}'
        """
               
        df = SparkClass.jdbc_reader.option("query", sql).load()
        
        return df
    
    def get_statistic_women_pop_sql(self, start_date, end_date) -> "pyspark.sql.DataFrame":
        """ PCELL시간대여성인구합계 통계데이터를 가져오는 쿼리 반환 함수

        [Column]
        stdr_de   : 기준 일자
        stdr_tm   : 기준 시간
        grid_id   : 그리드ID
        women_pop : PCELL시간대여자수
        """
        
        sql = f"""
        SELECT /* PCELL시간대여성연령인구합계 */
               stdr_de
             , stdr_tm
             , grid_id
             , CAST(pcel_tmzn_fml_co AS DOUBLE PRECISION) AS women_pop
          FROM SOSS.DW_PCEL_TMZN_FML_POP_SUM
         WHERE stdr_de BETWEEN '{start_date}' AND '{end_date}'
           AND gu_cd = '{self.gu_cd}'
        """
        
        df = SparkClass.jdbc_reader.option("query", sql).load()
        
        return df

    def get_bus_stop_cnt_sql(self) -> "pyspark.sql.DataFrame":
        """ 그리드별 버스 정류소 합계 데이터를 가져오는 쿼리 반환 함수

        [Column]
        grid_id      : 그리드ID
        bus_stop_cnt : 버스 정류소 개수
        """

        sql = f"""
        SELECT C.grid_id AS grid_id
             , COUNT(C.sttn_id) AS bus_stop_cnt
          FROM (
                SELECT A.grid_id
                     , B.sttn_id
                     , ROUND((6371 * ACOS(COS( RADIANS(A.cnt_la)) * COS(RADIANS(B.la)) * COS(RADIANS(B.lo) - RADIANS(A.cnt_lo)) + SIN(RADIANS(A.cnt_la) ) * SIN( RADIANS(B.la))))::NUMERIC, 2) AS distance
                  FROM (
                        SELECT /* PCELL기준정보 */
                               grid_id
                             , cnt_lo
                             , cnt_la
                          FROM SOSS.PCEL_STDR_INFO
                         WHERE admd_cd LIKE '{self.gu_cd}%'
                       ) A
                 CROSS JOIN (
                             SELECT /* 버스정류소 */
                                    sttn_id
                                  , crdnt_xaxs AS lo
                                  , crdnt_yaxs AS la
                               FROM DIL.DW_TRFC_BUS_STTN
                            ) B
               ) C
         WHERE C.distance <= 0.5
         GROUP BY C.grid_id
        HAVING COUNT(C.STTN_ID) > 0
        """
        
        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df
        
    def get_safe_idex_sql(self) -> "pyspark.sql.DataFrame":
        """ 안전지수 그리드 데이터 가져오는 쿼리 반환 함수

        [Column]
        stdr_tm   : 기준 시간
        grid_id   : 그리드ID
        admd_cd   : 행정동 코드
        safe_idex : 안전지수
        """

        sql = f"""
        SELECT /* 안전지수 그리드 */
               stdr_tm
             , grid_id
             , admd_cd
             , CAST(safe_idex AS FLOAT) AS safe_idex
          FROM SOSS.DM_SAFE_IDEX_GRID
         WHERE stdr_de = '{self.today}'
           AND gu_cd = '{self.gu_cd}'
        """
        
        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_land_per_sql(self) -> "pyspark.sql.DataFrame":
        """ 행정동별 토지 비율 데이터를 가져오는 쿼리 반환 함수

        [Column]
        admd_cd  : 행정동 코드
        land_cat : 용도지역의 분류 [ C1 : 주거지, C2: 상업지, C3: 공업지, C4: 그 외 나머지 ]
        land_per : 행정동별 용도지역의 분류 비율
        """
        
        sql = f"""
        SELECT DISTINCT admd_cd
             , land_cat
             , COUNT(*) OVER (PARTITION BY admd_cd, land_cat) / CAST(COUNT(*) OVER (PARTITION BY admd_cd) AS DOUBLE PRECISION) AS land_per
          FROM (
                SELECT /* 토지특성 */
                       admd_cd
                     , CASE WHEN spfc1 IN ('11', '12', '13', '14', '15') THEN 'cd1' --주거지
                            WHEN spfc1 IN ('21', '22', '23', '24') THEN 'cd2' --상업지
                            WHEN spfc1 IN ('31', '32', '33') THEN 'cd3' --공업지
                       ELSE 'cd4'
                       END AS land_cat
                  FROM SOSS.APMM_NV_LAND
                 WHERE sgg_cd = '{self.gu_cd}'
               ) A
        """
        
        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_npa_dcr_rcp_sql(self, start_month: str, end_month: str) -> "pyspark.sql.DataFrame":
        """ 시간별 행정동별 신고 수 데이터를 가져오는 쿼리 반환 함수

        [Column]
        stdr_dt : 기준일자 + 기준시간(HH:mm:ss)
        admd_cd : 행정동 코드
        all_cnt : 전체 범죄 수
        cnt     : 중요범죄 or 무거운 범죄 수
        """
        
        sql = f"""
        SELECT /* 경찰청신고접수 */
               rcp_tm AS stdr_tm
             , admd_cd
             , count(case when (incd_emr_cd in ('C0','C1')) then 1 end) as import_report_cnt
             , count(*) as all_report_cnt
          FROM SOSS.NPA_DCR_RCP
         WHERE gu_cd = '{self.gu_cd}'
           AND (rcp_de LIKE '____{start_month}__')
            OR (rcp_de LIKE '____{end_month}__')
         GROUP BY rcp_tm, admd_cd
        """
        
        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_cctv_cnt_sql(self) -> "pyspark.sql.DataFrame":
        """ 그리드별 CCTV 개수 데이터를 가져오는 쿼리 반환 함수

        [Column]
        grid_id : 그리드ID
        mtr_no  : 행렬번호
        cctv_co : CCTV대수
        """

        sql = f"""
        SELECT /* PCELL기준정보 */
               grid_id
             , row_no
             , clm_no
             , mtr_no
             , cctv_co
          FROM SOSS.PCEL_STDR_INFO
         WHERE SUBSTR(admd_cd, 1, 5) = '{self.gu_cd}'
        """
        
        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_pop_sum_sql(self, start_date: str, end_date: str) -> "pyspark.sql.DataFrame":
        """ PCELL단위시간대별유동인구 데이터를 가져오는 쿼리 반환 함수

        [Column]
        stdr_dt  : 기준일자 + 기준시간(HH:mm:ss)
        grid_id  : 그리드ID
        fpop_sum : 유동인구수 합계
        """

        sql = f"""
        SELECT /* PCELL단위시간대별유동인구 */
               grid_id
             , stdr_tm
             , CAST(SUM(fpop_co) AS DOUBLE PRECISION) AS fpop_sum
          FROM SOSS.DW_PCELL_TMZN_FPOP
         WHERE stdr_de BETWEEN '{start_date}' AND '{end_date}'
           AND gu_cd = '{self.gu_cd}'
         GROUP BY grid_id, stdr_tm
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    @staticmethod
    def insert_cctv_eff_idex_grid_sql(sparkDataFrame: "pyspark.sql.DataFrame") -> None:
        """ 안전지수그리드 데이터를 삽입하는 쿼리 반환 함수

        Paramters
        ----------
        sparkDataFrame: pyspark.sql.DataFrame
        """
      
        sparkDataFrame.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWRD) \
            .option("dbtable", "SOSS.DM_CCTV_EFF_IDEX_GRID") \
            .save(mode="append")

    @staticmethod
    def insert_cctv_eff_idex_admd_sql(sparkDataFrame: "pyspark.sql.DataFrame") -> None:
        """ 안전지수행정동 데이터를 삽입하는 쿼리 반환 함수

        Paramters
        ----------
        sparkDataFrame: pyspark.sql.DataFrame
        """

        sparkDataFrame.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWRD) \
            .option("dbtable", "SOSS.DM_CCTV_EFF_IDEX_ADMD") \
            .save(mode="append")

    @staticmethod
    def insert_cctv_eff_idex_sgg_sql(sparkDataFrame: "pyspark.sql.DataFrame") -> None:
        """ 안전지수시군구 데이터를 삽입하는 쿼리 반환 함수

        Paramters
        ----------
        sparkDataFrame: pyspark.sql.DataFrame
        """

        sparkDataFrame.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWRD) \
            .option("dbtable", "SOSS.DM_CCTV_EFF_IDEX_SGG") \
            .save(mode="append")