#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from soss.spark_session.db_connector import SparkClass
from soss.utils.config_parser import *

def delete_gu_cctv_mntr_rate(today: str):

    # define delete date
    delete_date = datetime.strptime(today, "%Y%m%d") - relativedelta(days=2)
    delete_date = datetime.strftime(delete_date, "%Y%m%d")
        
    # Create an engine to connect to the database    
    engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWRD}@{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    # 세션을 사용하여 데이터베이스 작업 수행
    with Session(engine) as session:
    
        table = "SOSS.DM_GU_CCTV_MNTR_RATE"
        
        try:
            # 데이터 삭제
            session.execute(f"DELETE FROM {table} WHERE stdr_de <= '{delete_date}'")
            time.sleep(10)
            
            session.execute(f"DELETE FROM {table} WHERE stdr_de = '{today}'")
            time.sleep(10)
            
            # 10초 후 commit
            session.commit()
            logging.info(f"DatMart {table} {delete_date} 이전 일자 Delete!")
            logging.info(f"DatMart {table} {today} 일자 Delete!")
        except:
            # 예외가 발생한 경우 롤백
            session.rollback()
            logging.error(f"Error raisd. Transaction Rollback!")
        
    engine.dispose()
        
def spark_insert_dm_gu_cctv_mntr_rate(arg_date: str):
    
    # logging setting
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    logging.info("Main-Process : Start")
    start = time.time()
    
    logging.info("-- spark load --")
    
    today = arg_date
    
    # 기존 일자에 데이터가 있다면 Delete 수행
    delete_gu_cctv_mntr_rate(today)
    
    sql = f"""
    SELECT '{today}' AS stdr_de -- 기준일자
           , gu_cd
           , admd_cd
           , cctv_tmzn_cd
           , ROUND(AVG(mntr_rate), 3) AS mntr_rate 
      FROM (
            SELECT stdr_de
                 , gu_cd
                 , admd_cd
                 , CASE WHEN stdr_tm IN ('07', '08', '09', '10', '11', '12', '13', '14') THEN '1'
                        WHEN stdr_tm in ('15', '16', '17', '18', '19', '20') THEN '2'
                   ELSE '3'
                   END AS cctv_tmzn_cd
                 , (cctv_eff_idex - MIN(cctv_eff_idex) OVER (PARTITION BY stdr_de, stdr_tm, gu_cd)) / (MAX(cctv_eff_idex) OVER (PARTITION BY stdr_de, stdr_tm, gu_cd) - MIN(cctv_eff_idex) OVER (PARTITION BY stdr_de, stdr_tm, gu_cd))*49 + 40 AS mntr_rate
              FROM SOSS.DM_CCTV_EFF_IDEX_ADMD
             WHERE stdr_de = '{today}'
           ) A
    GROUP BY stdr_de, gu_cd, admd_cd, cctv_tmzn_cd
    """

    df = SparkClass.jdbc_reader.option("query", sql).load()

    df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{DM_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWRD) \
        .option("dbtable", "SOSS.DM_GU_CCTV_MNTR_RATE") \
        .save(mode="append")

    end = time.time()
    elapsed_time = str(timedelta(seconds=end-start)).split('.')[0]
    logging.info(f"프로그램 소요시간 : {elapsed_time} --")
    logging.info("Main-Process : End")       