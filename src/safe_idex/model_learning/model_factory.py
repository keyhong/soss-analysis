# -*- coding: utf-8 -*-

from __future__ import annotations

from abc import abstractmethod
import logging
from collections import defaultdict
import os

import joblib
from lightgbm import LGBMRegressor
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from xgboost import XGBRegressor

MODEL_BASE_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model")

__all__ = [
    "ModelFactory",
    "XGBoost",
    "LGBM"
]

class ModelFactory:
    """ 분석 모델을 만들고, 학습시키기 위해 모델의 종류를 모아 놓은 Factory 클래스 """
    
    r_squared_dict = defaultdict(int)
    
    def __init__(
        self,
        train_x: pd.DataFrame,
        train_y: pd.DataFrame,
        predict_x: pd.DataFrame,
        predict_y: pd.DataFrame,
        fold: int
    ):
        # 학습 데이터
        self.train_x = train_x
        self.train_y = train_y

        # 테스트 데이터
        self.predict_x = predict_x
        self.predict_y = predict_y
        
        # 폴드
        self.fold = fold
        
    @abstractmethod
    def training(self):
        pass

    def add_model_r_square(self, model_nm: str, test_score: float) -> None:

        self.r_squared_dict[model_nm] += test_score

    @classmethod
    def select_best_model(cls, model_type: str, gu_nm: str):
        """03. 최적 모델을 선정하고, 폴드별 최적모델을 local에 저장하는 함수

        Parameters
        ----------
        model_type: str
           모델의 종류 (위해지표 모델 - hazard, 취약지표 모델 - weak)

        gu_nm: str
           구 이름
        """

        # value의 값에 따라 키 순서를 내림차순으로 정렬
        models = sorted(cls.r_squared_dict.items(), key=lambda x: x[1], reverse=True)
        
        logging.info(models)
        
        # r2_score가 가장 높은 모델 선택
        best_model = models[0][0]
        
        if best_model == "LinearRegeression":
            best_model = LinearRegeression.models

        if best_model == "RandomForestRegression":
            best_model = RandomForestRegression.models

        if best_model == "XGBoost":
            best_model = XGBoost.models

        if best_model == "LGBM":
            best_model = LGBM.models

        # 최적 학습모델 저장
        for idx, model in best_model.items():
            
            joblib.dump(
                value=model,
                filename=os.path.join(MODEL_BASE_FOLDER, model_type, gu_nm, f"{gu_nm}_{idx}.pkl"),
                compress=3
            )
        
        cls.initialize_model_evaluation()
    
    @classmethod
    def initialize_model_evaluation(cls):
        cls.r_squared_dict = defaultdict(int)
        logging.info("ModelFactory initialize complete.")
        
              
class LinearRegeression(ModelFactory):
    """Linear Regression"""
    
    models = {}

    def __init__(
        self,
        *args,
        **kwargs   
    ):
        self.__init__(*args)

        if self.__class__.__name__ not in self.r_squared_dict:
            super().r_squared_dict[self.__class__.__name__] = 0
            
    def training(self):
              
        # LinearRegression 인스턴스 초기화
        lm = LinearRegression()

        # 모델학습
        lm.fit(self.train_x, self.train_y)

        # LinearRegression 학습모델 저장
        self.LinearRegeression[self.fold] = lm

        # 로그 기록하기
        test_score = r2_score(self.predict_y, lm.predict(self.predict_x))

        return self.add_model_r_square(self.__class__.__name__, test_score)

class RandomForestRegression(ModelFactory):
    """Random Forest Regresssion"""
    
    models = {}
    
    def __init__(
        self,
        *args,
        **kwargs   
    ):
        self.__init__(*args)

        if self.__class__.__name__ not in self.r_squared_dict:
            super().r_squared_dict[self.__class__.__name__] = 0
            
    def training(self):

        # RandomForestRegressor 인스턴스 초기화
        rf_reg = RandomForestRegressor(max_depth=6) # RandomForest 모델선언

        # 모델 학습
        rf_reg.fit(self.train_x, self.train_y)

        # RandomForestRegressor 모델 저장
        self.RandomForestRegression[self.fold] = rf_reg

        # 로그 기록하기
        test_score = r2_score(self.predict_y, rf_reg.predict(self.predict_x))        

        return self.add_model_r_square(self.__class__.__name__, test_score)


class XGBoost(ModelFactory):
    """ XGBoost """
    
    models = {}

    def __init__(
        self,
        *args,
        **kwargs
    ):
        super().__init__(*args)

        if self.__class__.__name__ not in self.r_squared_dict:
            super().r_squared_dict[self.__class__.__name__] = 0
            
    def training(self):
                          
        # XGBRegressor 인스턴스 초기화
        xgb = XGBRegressor(max_depth=6, n_jobs=12)

        # 모델학습
        xgb.fit(self.train_x, self.train_y)

        # XGBRegressor 모델 저장
        self.models[self.fold] = xgb
        
        # 로그 기록하기
        # train_score = r2_score(self.train_y, xgb.predict(self.train_x))
        test_score = r2_score(self.predict_y, xgb.predict(self.predict_x))

        return self.add_model_r_square(self.__class__.__name__, test_score)

class LGBM(ModelFactory):
    """ LGBM """
    
    models = {}

    def __init__(
        self,
        *args,
        **kwargs   
    ):
        super().__init__(*args)
        
        if self.__class__.__name__ not in self.r_squared_dict:
            super().r_squared_dict[self.__class__.__name__] = 0
            
    def training(self):

        # LGBMRegressor 인스턴스 초기화
        lgbm = LGBMRegressor(num_leaves=64)

        # 모델학습
        lgbm.fit(self.train_x, self.train_y)

        # LGBMRegressor 모델 저장
        self.models[self.fold] = lgbm
        
        # 로그 기록하기
        # train_score = r2_score(self.train_y, lgbm.predict(self.train_x))
        test_score = r2_score(self.predict_y, lgbm.predict(self.predict_x))
        
        return self.add_model_r_square(self.__class__.__name__, test_score)
