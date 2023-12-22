#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from setuptools import setup, find_packages

try:
    exec(open("soss/_version.py").read())
except IOError:
    print(
        "Failed to load PySpark version file for packaging. You must be in SOSS's python dir.",
        file=sys.stderr,
    )
    sys.exit(-1)

VERSION = __version__


with open("README.rst") as f:
    long_description = f.read()

if __name__ == "__main__":
    
    setup(
        name="soss",
        version=VERSION,
        description='Soss Analysis Python Program',
        long_description=long_description,
        author="B2EN",
        author_email="b2en@b2en.com",
        license="MIT License",
        packages=find_packages(),
        install_requires=[
            "numpy",
            "pandas==1.4.1",
            "geopandas==0.13.2",
            "tdqm",
            "joblib==1.3.2",
            "scikit-learn",
            "xgboost",
            "lightgbm==3.3.5",
            "haversine",
            "pyspark==3.3.2",
            "apache-airflow[postgres]==2.6.3",
            "pyarrow==13.0.0"
        ],
        python_requires=">=3.6"
    )