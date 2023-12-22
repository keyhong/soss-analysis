# -*- coding: utf-8-*-

from soss.spark_session.db_connector import SparkClass
from soss.utils.config_parser import *

__all__ = ["SafeIdexQuery"]

class SafeIdexQuery:
    """ DW에서 SELECT하고, DM에 INSERT하는 쿼리 모음 클래스 """

    def __init__(self, today: str, gu_cd: str):
        self.today = today
        self.gu_cd = gu_cd

    def get_pcel_stdr_info_sql(self) -> "pyspark.sql.DataFrame":
        """ PCELL기준정보 데이터를 가져오는 쿼리 반환 함수

        [Column]
        grid_id : 그리드ID
        mtr_no  : 행렬번호
        admd_cd : 행정동코드
        row_no  : 행번호
        clm_no  : 열번호
        """

        sql = f"""
        SELECT /* PCELL기준정보 */
               grid_id
             , mtr_no
             , admd_cd
             , CAST(row_no AS SMALLINT) AS row_no
             , CAST(clm_no AS SMALLINT) AS clm_no
          FROM SOSS.PCEL_STDR_INFO
         WHERE admd_cd LIKE '{self.gu_cd}%'
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_npa_dcr_rcp_sql(self, start_month: str, end_month: str) -> "pyspark.sql.DataFrame":
        """ 경찰신고접수 정보 데이터를 가져오는 쿼리 반환 함수

        [Column]
        stdr_de     : 접수일자
        stdr_tm     : 접수시간
        incd_emr_cd : 사건긴급코드
        incd_ass_nm : 사건종별명
        mtr_no      : 행렬번호
        """

        sql = f"""
        SELECT /* 경찰청신고접수 */
               rcp_de AS stdr_de
             , rcp_tm AS stdr_tm
             , mtr_no
             , incd_emr_cd
          FROM SOSS.NPA_DCR_RCP
         WHERE gu_cd = '{self.gu_cd}'
           AND (rcp_de LIKE '____{start_month}__')
            OR (rcp_de LIKE '____{end_month}__')
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_wth_frcs_sql(self, start_date: str, end_date: str) -> "pyspark.sql.DataFrame":
        """ 날씨예보 데이터를 가져오는 쿼리 반환 함수

        [Column]
        stdr_de : 기준일자
        stdr_tm : 시간대
        admd_cd : 행정동코드
        hd_val  : 습도값
        atp_val : 기온값
        """

        sql = f"""
        SELECT B.frcst_de AS stdr_de
             , SUBSTR(B.frcst_time, 1, 2) AS stdr_tm
             , B.admd_cd
             , CAST(SUM(CASE WHEN B.frcst_dta_ty = 'T1H' THEN frcst_value ELSE 0 END) AS DOUBLE PRECISION) AS atp_val -- 기온값
             , CAST(SUM(CASE WHEN B.frcst_dta_ty = 'REH' THEN frcst_value ELSE 0 END) AS DOUBLE PRECISION) AS hd_val -- 습도값
          FROM (
                SELECT A.frcst_de
                     , A.frcst_time
                     , A.admd_cd
                     , A.frcst_dta_ty
                     , A.frcst_value
                  FROM (
                        SELECT /* 기상청단기예보조회 */
                               frcst_de
                             , frcst_time
                             , frcst_dta_ty
                             , admd_cd
                             , frcst_value
                             , ROW_NUMBER() OVER (PARTITION BY frcst_de, frcst_time, frcst_dta_ty, admd_cd ORDER BY stdr_de desc, stdr_time DESC) AS ROW_NUMBER
                          FROM DIL.DW_KMA_SRTPD_FRCST_INQIRE
                         WHERE frcst_de BETWEEN '{start_date}' AND '{end_date}'
                           AND frcst_dta_ty IN ('T1H', 'REH')
                           AND admd_cd LIKE '{self.gu_cd}%'
                       ) A
                 WHERE A.ROW_NUMBER = 1
               ) B
          GROUP BY B.frcst_de, B.frcst_time, B.admd_cd
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_tmp_wth_frcs_sql(self) -> "pyspark.sql.DataFrame":
        """ 날씨예보(보간) 데이터를 가져오는 쿼리 반환 함수 (null인 값을 대체하기 위한 데이터)

        [Column]
        stdr_de : 기준일자
        stdr_tm : 시간대
        admd_cd : 행정동코드
        hd_val  : 습도값
        atp_val : 기온값
        """

        sql = f"""
        SELECT /* 날씨예보 */
               stdr_de
             , tmzn AS stdr_tm
             , admd_cd
             , CAST(hd_val AS DOUBLE PRECISION) AS hd_val
             , CAST(tmp_val AS DOUBLE PRECISION) AS atp_val
          FROM SOSS.WTH_FRCS
         WHERE admd_cd LIKE '{self.gu_cd}%'
        """
        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_dw_pcell_tmzn_fpop_sql(self, month: str) -> "pyspark.sql.DataFrame":
        """ PCELL단위시간대별유동인구 데이터를 가져오는 쿼리 반환 함수

        [Column]
        stdr_de : 기준일자
        stdr_tm : 기준시간
        grid_id : 그리드ID
        fpop_co : 유동인구수
        """

        sql = f"""
        SELECT /* PCELL단위시간대별유동인구 */
               stdr_de
             , stdr_tm
             , grid_id
             , gu_cd
             , CAST(fpop_co AS DOUBLE PRECISION) AS weak_score
          FROM SOSS.DW_PCELL_TMZN_FPOP_2022_{month}
         WHERE gu_cd = '{self.gu_cd}'
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    def get_pcel_cctv_cnt_sql(self) -> "pyspark.sql.DataFrame":
        """ PCELL기준정보의 CCTV 개수 데이터를 가져오는 쿼리 반환 함수

        [Column]
        grid_id : 그리드ID
        admd_cd : 행정동코드
        cctv_co : CCTV대수
        """

        sql = f"""
        SELECT /* PCELL기준정보 */
               grid_id
             , admd_cd
             , cctv_co
          FROM SOSS.PCEL_STDR_INFO
         WHERE admd_cd LIKE '{self.gu_cd}%'
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    @staticmethod
    def insert_safe_idex_grid_sql(sparkDataFrame: "pyspark.sql.DataFrame") -> None:
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
                 .option("dbtable", "SOSS.DM_SAFE_IDEX_GRID") \
                 .save(mode="append")

    @staticmethod
    def insert_safe_idex_admd_sql(sparkDataFrame: "pyspark.sql.DataFrame") -> None:
        """ 안전지수행정동 데이터를 삽입하는 쿼리 반환 함수

        Paramters
        ----------
        sparkDataFrame : pyspark.sql.DataFrame
        """

        sparkDataFrame.write.format("jdbc") \
                 .option("url", f"jdbc:postgresql://{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}") \
                 .option("driver", "org.postgresql.Driver") \
                 .option("user", POSTGRES_USER) \
                 .option("password", POSTGRES_PASSWRD) \
                 .option("dbtable", "SOSS.DM_SAFE_IDEX_ADMD") \
                 .save(mode="append")

    @staticmethod
    def insert_safe_idex_sgg_sql(sparkDataFrame: "pyspark.sql.DataFrame") -> None:
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
                 .option("dbtable", "SOSS.DM_SAFE_IDEX_SGG") \
                 .save(mode="append")