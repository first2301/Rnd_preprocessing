# 필요한 라이브러리 import
import os
import glob
import time
import json
import psutil
import sqlite3
import logging
import subprocess
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import StringType, LongType, StructType, StructField

from lib.prepro import Preprocessing, SparkDataFrame # 직접 개발한 패키지


start_time = time.time() # 프로그램 시작 시간 기록
# spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder \
    .appName("ImageDataProcessing") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


prepro = Preprocessing() # 데이터 전처리 라이브러리 호출
# logging 모드 설정: logging.INFO / logging.DEBUG
log_file = './log/prepro.log' # 데이터 처리 로그 저장 경로
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
logging.info('데이터 처리 시작')
# prepro.log_system_resources() # 시스템 리소스 확인

# data_dir = os.path.abspath('./')
# data_path = subprocess.run(['tree', '-d', data_dir], text=True, capture_output=True, check=True) # 전체 데이터 디렉토리 정보 확인
# if data_path.returncode == 0:
#     logging.info("데이터 디렉토리 확인:\n%s", data_path.stdout) # 로그 파일에 디렉토리 정보 출력

# Data path 
PATH_CCTV_DATA = 'G:/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
PATH_SAND_DATA = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
PATH_SAND_LABEL = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'

# img_paths = prepro.extracted_path(paths)
# 데이터 경로 리스티(list) 형태로 추출
cctv_data = prepro.get_all_file_paths(PATH_CCTV_DATA)
sand_data = prepro.get_all_file_paths(PATH_SAND_DATA)
sand_label_data = prepro.get_all_file_paths(PATH_SAND_LABEL)

# 이미지 데이터에서 메타 데이터 추출
logging.info('메타 데이터 추출 시작..') # 데이터 처리 및 데이터프레임 생성

sparkdf = SparkDataFrame()
cctv_df = sparkdf.get_spark_dataframe(cctv_data)
sand_df = sparkdf.get_spark_dataframe(sand_data)
sand_label_df = sparkdf.get_spark_dataframe(sand_label_data)

logging.info('메타 데이터 추출 완료')

logging.info('테이블 생성 시작..')

# updated_df.set_index(merge_df['file_id']) # test

logging.info('데이터 테이블 생성 완료') 
logging.info(f"Table row: {cctv_df.count()}, Table columns: {len(cctv_df.columns)}")
logging.info(f"Table row: {sand_df.count()}, Table columns: {len(sand_df.columns)}")
logging.info(f"Table row: {sand_label_df.count()}, Table columns: {len(sand_label_df.columns)}")

# 데이터 타입별로 counts 확인
# prepro.ptint_data_info(merge_df) # 함수 내에서 로그 출력

# 데이터베이스 생성 및 저장
# prepro.save_data(updated_df)

# 전체 디렉토리 확인
prepro.check_total_dir()

end_time = time.time()  # 실행 후 시간을 기록
elapsed_time = end_time - start_time  # 경과된 시간 계산
minutes, seconds = divmod(elapsed_time, 60) # ms를 분, 초로 변환
logging.info("경과 시간: {}분 {}초".format(int(minutes), int(seconds))) # 분, 초로 변환한 데이터 로깅
logging.info('데이터 처리 종료') 

