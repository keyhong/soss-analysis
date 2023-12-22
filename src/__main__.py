#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse

def date_bandwidth_type(x):
    
    str_x = str(x)
    if len(str_x) != 8:
        raise argparse.ArgumentTypeError("date format is 'YYYYMMDD'")
    
    return str_x

def main():
    
    parser = argparse.ArgumentParser(prog="soss")
    
    # service_subparser
    service_subparser = parser.add_subparsers(metavar="SERVICE", dest="service", required=True)
    
    # safe_idex
    service_subparser.add_parser("safe-idex-preprocessing", help='안전지수 전처리')
    service_subparser.add_parser("safe-idex-model-learning", help='안전지수 모델 학습')
    service_subparser.add_parser("safe-idex-operation", help='안전지수 운영')

    # cctv_optmz
    service_subparser.add_parser("cctv-optmz-model-learning", help='CCTV 최적화 모델 학습')
    service_subparser.add_parser("cctv-optmz-operation", help='CCTV 최적화 운영')

    # ptr_pst_rcm
    service_subparser.add_parser("ptr-pst-rcm-operation", help="순찰거점 운영")
    
    # statistic_etl
    service_subparser.add_parser("spark_insert_dm_gu_cctv_mntr_tm", help="CCTV 구별 시간 통계 적재")
    service_subparser.add_parser("spark_insert_dm_gu_cctv_mntr_rate", help="CCTV 구별 비율 통계 적재")    

    parser.add_argument(metavar="EXECUTION_DATE", dest="date", type=date_bandwidth_type)
    parser.add_argument(metavar="GU_NAME", dest="gu_nm", type=str)

    args = parser.parse_args()

    if args.service == "safe-idex-preprocessing":
        from soss.safe_idex.preprocessing.safe_idex_preprocessing import safe_idex_preprocessing_main
        main = safe_idex_preprocessing_main(args.date, args.gu_nm)
    
    if args.service == "safe-idex-model-learning":
        from soss.safe_idex.model_learning.safe_idex_model_learning import safe_idex_model_learning_main
        main = safe_idex_model_learning_main(args.date, args.gu_nm)

    if args.service == "safe-idex-operation":
        from soss.safe_idex.operation.safe_idex_operation import safe_idex_operation_main
        main = safe_idex_operation_main(args.date, args.gu_nm)        

    if args.service == "cctv-optmz-model-learning":
        from soss.cctv_optmz.model_learning.cctv_optmz_model_learning import cctv_optmz_model_learning_main
        main = cctv_optmz_model_learning_main(args.date, args.gu_nm)
        
    if args.service == "cctv-optmz-operation":
        from soss.cctv_optmz.operation.cctv_optmz_operation import cctv_optmz_operation_main
        main = cctv_optmz_operation_main(args.date, args.gu_nm)     

    if args.service == "ptr-pst-rcm-operation":
        from soss.ptr_pst_rcm.operation.ptr_pst_rcm_operation import ptr_pst_rcm_operation_main
        main = ptr_pst_rcm_operation_main(args.date)        
                
    if args.service == "spark_insert_dm_npa_dcr_rcp":
        from soss.statistic.spark_insert_dm_npa_dcr_rcp import spark_insert_dm_npa_dcr_rcp
        main = spark_insert_dm_npa_dcr_rcp(args.date)                

    if args.service == "spark_insert_dm_gu_cctv_mntr_tm":
        from soss.statistic.spark_insert_dm_gu_cctv_mntr_tm import spark_insert_dm_gu_cctv_mntr_tm
        main = spark_insert_dm_gu_cctv_mntr_tm(args.date)

    if args.service == "spark_insert_dm_gu_cctv_mntr_rate":
        from soss.statistic.spark_insert_dm_gu_cctv_mntr_rate import spark_insert_dm_gu_cctv_mntr_rate
        main = spark_insert_dm_gu_cctv_mntr_rate(args.date)


if __name__ == "__main__":
    sys.exit(main())
