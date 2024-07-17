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

from lib.prepro import Preprocessing


start_time = time.time() # 프로그램 시작 시간 기록


# spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder \
    .appName("ImageDataProcessing") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


prepro = Preprocessing
# logging 모드 설정: logging.INFO / logging.DEBUG
log_file = './log/prepro.log' # 데이터 처리 로그 저장 경로
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
logging.info('데이터 처리 시작')
prepro.log_system_resources() # 시스템 리소스 확인

data_dir = './data/' 
data_path = subprocess.run(['tree', '-d', data_dir], text=True, capture_output=True, check=True) # 전체 데이터 디렉토리 정보 확인
if data_path.returncode == 0:
    logging.info("데이터 디렉토리 확인:\n%s", data_path.stdout) # 로그 파일에 디렉토리 정보 출력

# Data path 추출
# G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터:/home/jovyan/work/data/sand
paths = 'data/sand/'
img_paths = prepro.extracted_path(paths)

# 이미지 데이터에서 메타 데이터 추출
logging.info('메타 데이터 추출 시작..') # 데이터 처리 및 데이터프레임 생성

get_file_id_udf = udf(prepro.get_file_id, StringType())
get_file_name_udf = udf(prepro.get_file_name, StringType())
get_folder_name_udf = udf(prepro.get_folder_name, StringType())
get_file_size_udf = udf(prepro.get_file_size, LongType())
df = spark.createDataFrame([(path,) for path in img_paths], ["full_path"])
df = df.withColumn("file_id", get_file_id_udf("full_path"))
df = df.withColumn("file_name", get_file_name_udf("full_path"))
df = df.withColumn("folder_name", get_folder_name_udf("full_path"))
df = df.withColumn("file_size", get_file_size_udf("full_path"))


label_data = glob.glob('./data/label_data/*.json') # 메타 데이터 경로
label_df = pd.DataFrame({"file_id": [label.split('.')[-2].split('/')[-1] for label in label_data],
                        "label_paths": label_data,})
logging.info('메타 데이터 추출 완료')

logging.info('테이블 생성 시작..')
prepro = Preprocessing
json_meta_data = prepro.normalize_json(label_df['label_paths']) # json 데이터를 pandas DataFrame로 변경

# json에서 pandas DataFrame로 변환한 여러 데이터를 하나의 DataFrame로 concat해서 종합
concat_df = pd.concat(json_meta_data, ignore_index=True) 

# 데이터 정보 
'''
Dataset.quality1: 품질정보1(밀도) / 2.5 이상
Dataset.quality2: 품질정보2(흡수율) / 3% 이하 (흡수율이 높으면 콘크리트 물비 증가 → 강도저하 및 균열 문제 발생)
Dataset.quality3: 품질정보3(안전성) / 5% 이하 (외부 화학적, 기상적 환경영향성 → 내구성 문제 발생)
Dataset.quality4: 품질정보4(잔입자 통과량) / 잔입자량이 많으면 콘크리트 물비 증가 → 강도저하 및 균열 문제 발생)
'''
# 데이터 선별
new_df = concat_df[['id', 'image.size.width', 'image.size.height', 'dataset.quality1', 'dataset.quality2', 'dataset.quality3', 'dataset.quality4', 'dataset.result']]
new_df.columns = ['file_id', 'width', 'height', '밀도', '흡수율', '안전성', '잔입자_통과량', 'target_data']
merge_df = pd.merge(df, new_df, on='file_id') # 이미지에서 추출한 메타 데이터와 json에서 DataFrame으로 변환한 데이터를 inner join

updated_df = merge_df.drop('file_id', axis=1)
# updated_df.set_index(merge_df['file_id']) # test

logging.info('데이터 테이블 생성 완료') 
logging.info(f"Table row: {len(updated_df)}, Table columns: {len(updated_df.columns)}")

# 데이터 타입별로 counts 확인
prepro.ptint_data_info(merge_df) # 함수 내에서 로그 출력

# 데이터베이스 생성 및 저장
prepro.save_data(updated_df)

# 전체 디렉토리 확인
prepro.check_total_dir()

end_time = time.time()  # 실행 후 시간을 기록
elapsed_time = end_time - start_time  # 경과된 시간 계산
minutes, seconds = divmod(elapsed_time, 60) # ms를 분, 초로 변환
logging.info("경과 시간: {}분 {}초".format(int(minutes), int(seconds))) # 분, 초로 변환한 데이터 로깅
logging.info('데이터 처리 종료') 