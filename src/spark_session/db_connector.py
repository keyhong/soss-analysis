"""
spark 이용하여 db 연결하는 모듈
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from soss.utils.config_parser import *

__all__ = ["SparkClass"]

class SparkClass:
    """ Spark을 Postgresql에 연결한다 """
    
    spark = SparkSession.builder \
        .appName("safe2.0") \
        .master(SPARK_MASTER) \
        .config("spark.jars", os.path.join(os.path.dirname(os.path.abspath(__file__)), "postgresql-42.6.0.jar")) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.maxResultSize", SPARK_MAX_RESULT_SIZE) \
        .config("spark.local.dir", SPARK_LOCAL_DIR) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    jdbc_reader = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DW_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWRD)

    def __del__(cls):
        cls.spark.stop()
