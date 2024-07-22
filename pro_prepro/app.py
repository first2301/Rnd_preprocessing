import streamlit as st
import pandas as pd
import time, requests, json, os
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, LongType

from lib.prepro import Preprocessing, SparkDataFrame


start_time = time.time()

spark = SparkSession.builder \
        .appName("large_dataset") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.executor.instances", "20") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "2000") \
        .getOrCreate()

prepro = Preprocessing() # 데이터 전처리 라이브러리 호출
sdf = SparkDataFrame()

PATH_CCTV_DATA = 'G:/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
PATH_SAND_DATA = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
PATH_SAND_LABEL = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'

df = pd.DataFrame({'path_id': [PATH_CCTV_DATA, PATH_SAND_DATA, PATH_SAND_LABEL]})
st.write(df)

# sand_label_sdf = sdf.get_spark_dataframe(PATH_SAND_LABEL)
# pdf = sand_label_sdf.toPandas()
# st.write(pdf)


@staticmethod
@udf(StringType())
def extract_file_id(paths):
    '''file_id 추출'''
    file_name = os.path.basename(paths)
    file_id = os.path.splitext(file_name)[0]
    return file_id

@staticmethod
@udf(StringType())
def extract_file_name(paths):
    '''file_name 추출'''
    return os.path.basename(paths)

@staticmethod
@udf(StringType())
def extract_folder_name(paths):
    '''folder_name 추출'''
    return os.path.basename(os.path.dirname(paths))

@staticmethod
@udf(LongType())
def extract_file_size(paths):
    '''file_size 추출'''
    return os.path.getsize(paths)

def get_spark_dataframe(paths):
    '''spark_dataframe 생성'''
    df = spark.createDataFrame([(path,) for path in paths], ["full_path"])
    df = df.withColumn("file_id", extract_file_id("full_path"))
    df = df.withColumn("file_name", extract_file_name("full_path"))
    df = df.withColumn("folder_name", extract_folder_name("full_path"))
    df = df.withColumn("file_size", extract_file_size("full_path"))
    return df


path = prepro.get_all_file_paths(PATH_SAND_LABEL)
spakr_df = get_spark_dataframe(path)
df = spakr_df.toPandas()
st.write(df)

# response = requests.post('http://127.0.0.1:8000/prepro', json=PATH_SAND_DATA)

# if response.status_code == 200:
#     json_data = response.json()
#     data = json.load(json_data)
#     st.write(data)

# cctv_data = prepro.get_all_file_paths(PATH_CCTV_DATA)
# sand_data = prepro.get_all_file_paths(PATH_SAND_DATA)
# sand_label_data = prepro.get_all_file_paths(PATH_SAND_LABEL)

# if st.button():
    # st.write(cctv_data)
    # st.write(sand_data)
    # st.write(sand_label_data)