# -*- coding: utf-8-*-

from soss.spark_session.db_connector import SparkClass
from soss.utils.config_parser import *

__all__ = ["PtrPstRcmQuery"]

class PtrPstRcmQuery:
    """ DW에서 SELECT하고, DM에 INSERT하는 쿼리 모음 클래스 """
    
    def __init__(self, today: str, gu_cd: str):
        self.today = today
        self.gu_cd = gu_cd

    def get_dm_safe_idex_grid_sql(self) -> "pyspark.sql.DataFrame":
        """ 안전지수 데이터를 가져오는 쿼리 반환 함수

        [Column]
        ptr_tmzn_cd : 순찰시간대
        grid_id     : 그리드ID
        safe_idex   : 안전지수
        """

        sql = f"""
        SELECT /* 안전지수그리드 */
                 CAST(stdr_tm AS INT) AS stdr_tm
               , grid_id
               , CAST(safe_idex AS DOUBLE PRECISION) AS safe_idex
          FROM SOSS.DM_SAFE_IDEX_GRID
         WHERE stdr_de = '{self.today}'
           AND gu_cd = '{self.gu_cd}'
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df
    
    def get_pcel_stdr_info_sql(self) -> "pyspark.sql.DataFrame":
        """ PCELL기준정보를 가져오는 쿼리 반환 함수

        [Column]
        grid_id   : 그리드ID
        mtr_no    : 행렬번호
        admd_cd   : 행정동코드
        cnt_x_crd : 중심X좌표
        cnt_y_crd : 중심Y좌표
        """

        sql = f"""
        SELECT /* PCELL기준정보 */
               grid_id
             , mtr_no
             , admd_cd
             , CAST(cnt_x_crd AS DOUBLE PRECISION) AS cnt_x_crd
             , CAST(cnt_y_crd AS DOUBLE PRECISION) AS cnt_y_crd
          FROM SOSS.PCEL_STDR_INFO
         WHERE admd_cd LIKE '{self.gu_cd}%'
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df
    
    def get_distinct_grid_id_sql(self, start_date: str, end_date: str) -> "pyspark.sql.DataFrame":
        """ PCELL시간대유동인구 중복제거 그리드ID 데이터를 가져오는 쿼리 반환 함수

        [Column]
        grid_id : 그리드ID (유니크값)
        """

        sql = f"""
        SELECT /* PCELL시간대유동인구 */
               DISTINCT grid_id
          FROM SOSS.DW_PCELL_TMZN_FPOP
         WHERE stdr_de BETWEEN '{start_date}' AND '{end_date}'
           AND gu_cd = '{self.gu_cd}'
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    @staticmethod
    def get_admd_plbx_mapn_sql() -> "pyspark.sql.DataFrame":
        """ 행정동지구대매핑 데이터를 가져오는 쿼리 반환 함수

        [Column]
        admd_cd     : 행정동코드
        gu_cd       : 구코드
        plbx_nm     : 지구대명
        ptr_vhcl_co : 순찰차량수
        """

        sql = f"""
        SELECT /* 행정동지구대매핑 */
               admd_cd
             , plbx_nm
             , ptr_vhcl_co
          FROM SOSS.ADMD_PLBX_MAPN
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    @staticmethod
    def get_plbx_info_sql() -> "pyspark.sql.DataFrame":
        """ 지구대 정보 데이터 불러오는 쿼리 반환 함수

        [Column]
        plbx_nm : 지구대명
        gu_cd   : 구코드
        la      : 위도
        lo      : 경도
        """

        sql = f"""
        SELECT /* 지구대 정보 */
               plbx_nm
             , gu_cd
             , CAST(ROUND(la, 6) AS DOUBLE PRECISION) AS plbx_la
             , CAST(ROUND(lo, 6) AS DOUBLE PRECISION) AS plbx_lo
          FROM SOSS.PLBX_INFO
        """

        df = SparkClass.jdbc_reader.option("query", sql).load()

        return df

    @staticmethod
    def insert_ptr_pst_sql(sparkDataFrame: "pyspark.sql.DataFrame") -> None:
        """ 순찰거점추천 데이터를 삽입하는 쿼리 반환 함수

        Paramters
        ----------
        sparkDataFrame: pyspark.sql.DataFrame
        """

        sparkDataFrame.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWRD) \
            .option("dbtable", "SOSS.DM_PTR_PST") \
            .save(mode="append")                   