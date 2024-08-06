import os
import json
import time
import glob
import psutil
import sqlite3
import logging
import zipfile
import subprocess
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import dask
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.context import SparkContext
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, LongType, StructType

import findspark
findspark.init()

class Preprocessing:
    def __init__(self):
        # self.spark = SparkSession.builder \
        #             .config("spark.driver.memory", "4g") \
        #             .config("spark.executor.memory", "4g") \
        #             .getOrCreate()
        # self.sc = SparkContext('local').setLogLevel("WARN")
        pass
        
    def format_bytes(self, size): # 파일 용량 계산
        '''
        byte를 KB, MB, GB, TB 등으로 변경하는 함수
        '''
        volum = 1024
        n = 0
        volum_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
        size - 0
        while tqdm(size > volum):
            size /= volum
            n += 1
        # return f"{size:.5f} {volum_labels[n]}"
        return f"{size} {volum_labels[n]}"

    # def normalize_json(self, json_meta_data): # json 형태의 데이터를 DataFrame으로 변환
    #     '''
    #     json데이터를 pandas DataFrame로 변경하는 함수
    #     '''
    #     json_df = []
    #     for file in tqdm(json_meta_data):
    #         with open(file, 'r', encoding='utf-8') as f:
    #             data = json.load(f)
    #             data_df = pd.json_normalize(data)
    #             json_df.append(data_df)
    #     return json_df


    
    # @staticmethod
    # def normalize_json(self, json_paths):
    #     '''json 파일들을 Pyspark DataFrame으로 변환'''
    #     df_list = []
    #     for file in json_paths:
    #         # JSON 파일을 Spark DataFrame으로 읽기
    #         df = self.spark.read.json(file)
    #         df = df.withColumn("file_id", self.extract_file_id()(df["full_path"]))
            
    #         df_list.append(df)
        
    #     return df_list

    def normalize_json(self, json_meta_data):
        '''
        JSON 파일 경로 리스트를 입력받아 DataFrame으로 변환하는 함수
        '''
        # 빈 리스트를 만들어서 DataFrame을 저장
        df_list = []
    
        for file in json_meta_data:
            # JSON 파일을 DataFrame으로 읽기
            df = self.spark.read.json(file, multiLine=True)
    
            # Flattening the DataFrame if needed
            # 필요한 경우 중첩된 구조를 평탄화
            flattened_df = self.flatten_df(df)
            
            df_list.append(flattened_df)
            
            return df_list

    def flatten_df(self, df):
        """
        중첩된 JSON 구조를 평탄화하는 함수
        """
        # 리스트 형태의 중첩 구조를 평탄화
        def flatten(x):
            flat_dict = {}
            for field in x:
                if isinstance(x[field], dict):
                    for subfield in x[field]:
                        flat_dict[f"{field}_{subfield}"] = x[field][subfield]
                else:
                    flat_dict[field] = x[field]
            return flat_dict

        # Spark DataFrame의 각 레코드에 대해 평탄화 적용
        rdd = df.rdd.map(lambda row: flatten(row.asDict()))
        flattened_df = self.spark.createDataFrame(rdd)

        return flattened_df
    
    def log_system_resources(self):
        '''
        불필요한 자원이 사용되고 있는지 확인하는 함수
        '''
        format_bytes = self.format_bytes()
        # 시스템 리소스 정보를 얻기 위해 psutil 사용
        memory_info = psutil.virtual_memory() # 메모리 정보
        cpu_percent = psutil.cpu_percent(interval=1)  # CPU 사용량 (1초 간격)
        disk_usage = psutil.disk_usage('/')  # 루트 디스크 사용량 정보
        
        current_time = time.strftime('%Y-%m-%d %H:%M:%S') # 현재 시간 기록
        # 시스템 리소스 정보를 표 형태로 생성
        system_info = f"""
        사용 중인 자원 확인:
        --------------------------------------
        전체 메모리: {format_bytes(memory_info.total)} 
        사용 가능한 메모리: {format_bytes(memory_info.available)} 
        사용된 메모리: {format_bytes(memory_info.used)} 
        메모리 사용 퍼센트: {memory_info.percent}%
        CPU 사용 퍼센트: {cpu_percent}%
        전체 디스크 용량: {format_bytes(disk_usage.total)} 
        사용된 디스크 용량: {format_bytes(disk_usage.used)} 
        디스크 사용 퍼센트: {disk_usage.percent}%
        --------------------------------------
        """ # 각주가 아니라 log 출력되는 comment
        logging.info(system_info)

    def extracted_path(self, paths):
        logging.info('데이터 로딩')
        img_dir_list = [path for path in paths if not path.endswith('.zip')]
        img_total_list = [glob.glob(folder+'/*') for folder in img_dir_list]
        img_paths = [img_path for sublist in img_total_list for img_path in sublist]
        return img_paths
    
    def scan_directory(self, path):
        # 스택을 초기화하고 시작 디렉터리를 추가
        stack = [path]
        total_paths = []
        while stack:
            current_path = stack.pop()
            
            with os.scandir(current_path) as it:
                for entry in it:
                    if entry.is_file() and not entry.name.endswith('.zip'):
                        total_paths.append(entry.path)
                    elif entry.is_dir(): # 디렉터리인 경우 스택에 추가
                        stack.append(entry.path)
        return total_paths

    def get_all_file_paths(self, root_path):
        paths = glob.glob(root_path)
        img_dir_list = [path for path in paths if not path.endswith('.zip')]
        file_paths = []
        for root_dir in img_dir_list:
            for dirpath, dirnames, filenames in os.walk(root_dir):
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    file_paths.append(full_path)
        return file_paths

    def get_file_name(self, path):
        return os.path.basename(path)
    
    def get_file_id(self, path):
        file_name = os.path.basename(path)
        file_id = os.path.splitext(file_name)[0]
        return file_id
        # return file_name.split('.')[0] 
        # return os.path.splitext(file_name)[0]
        
    def get_folder_name(self, path):
        return os.path.basename(os.path.dirname(path))
    
    def get_file_size(self, path):
        return os.path.getsize(path)
    
    def print_data_type_num(self, path):
        # 데이터 타입별로 counts 확인
        data_types = ['png', 'jpg', 'jpeg', 'etc', 'json']
        png_num = 0
        jpg_num = 0
        csv_num = 0
        json_num = 0
        etc = 0
        type_data = [] 
        for data in path:
            type_data.append(data.split('.')[-1])
        
        for idx, data_types in enumerate(type_data):
            if data_types == 'png':
                png_num += 1
            elif data_types == 'jpg' or data_types == 'jpeg':
                jpg_num += 1
            elif data_types == 'csv':
                csv_num += 1
            elif data_types == 'json':
                json_num += 1
            else:
                etc += 1

        file_info = f"""
            데이터 처리 정보: 
            -------------------------------------- 
            전체 이미지 데이터 수: {len(path)} 
            png counts: {png_num} 
            jpg counts: {jpg_num} 
            csv counts: {csv_num}
            json counts: {json_num}
            etc counts: {etc} 
            -------------------------------------- 
            """
        return file_info


    def ptint_data_info(self, merge_df):
        # 데이터 타입별로 counts 확인
        data_types = ['png', 'jpg', 'jpeg', 'etc']
        png_num = 0
        jpg_num = 0
        csv_num = 0
        etc = 0
        type_data = [] 
        for data in merge_df['full_path']:
            type_data.append(data.split('.')[-1])
        
        for idx, data_types in enumerate(type_data):
            if data_types == 'png':
                png_num += 1
            elif data_types == 'jpg' or data_types == 'jpeg':
                jpg_num += 1
            elif data_types == 'csv':
                csv_num += 1
            else:
                etc += 1
        
        # 처리한 데이터 정보 출력
        file_size = sum(merge_df['file_size']) # 전체 데이터 용량 sum
        file_info = f"""
                    데이터 처리 정보:
                    --------------------------------------
                    전체 이미지 데이터 수: {len(merge_df)}
                    전체 이미지 데이터 용량: {self.format_bytes(file_size)}
                    png counts: {png_num} 
                    jpg counts: {jpg_num}
                    etc counts: {etc}
                    --------------------------------------
                    """
        logging.info(file_info) # 데이터 처리 정보 확인
    
    def save_data(self, updated_df):
        db_path = './database/database.db' # sqlite3 데이터베이스 경로
        conn = sqlite3.connect(db_path)
        logging.info('데이터베이스 생성')
        
        if Path(db_path).exists():
            logging.info("데이터베이스 경로: %s", db_path)
        # 데이터 베이스에 데이터 저장
        updated_df.to_sql('TB_meta_info', conn, if_exists='replace', index=False) # pandas 데이터프레임을 db에 저장
        logging.info('데이터베이스에 데이터 저장 완료')
        conn.close() 
        logging.info('데이터베이스 연결 종료')
        
    def check_total_dir(self):
        current_directory = Path.cwd() 
        # tree 명령어 실행 (현재 디렉토리만 포함)
        total_dir = subprocess.run(['tree', '-d', current_directory], text=True, capture_output=True, check=True)
        # 실행 결과를 로그 파일에 저장
        if total_dir.returncode == 0:
            logging.info("전체 디렉토리 구조:\n%s", total_dir.stdout)
    
    # @dask.delayed
    def extract_zip(self, zip_file):
        destination = zip_file.with_suffix('')  # 확장자 제거한 경로 생성
        with zipfile.ZipFile(zip_file, 'r') as zip_data:
            zip_data.extractall(destination)
        return f"Extracted {zip_file} to {destination}"
    
    def extract_zip_in_list(zip_files):
        results = []
        for zip_file in zip_files:
            destination = zip_file.with_suffix('')  # 확장자 제거한 경로 생성
            with zipfile.ZipFile(zip_file, 'r') as zip_data:
                zip_data.extractall(destination)
            results.append(destination)
            
        return results

class SparkDataFrame:
    '''
    spark dataframe 생성
    '''
    def __init__(self):
        findspark.init()
        conf = SparkConf() \
            .setAppName("large_dataset") \
            .set("spark.driver.memory", "8g") \
            .set("spark.executor.memory", "8g") \
            .set("spark.executor.cores", "4") \
            .set("spark.jars", "/usr/local/spark/jars/postgresql-42.7.3.jar")
        # SparkSession 생성
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
    @staticmethod
    def spark_to_pandas(df):
        return df.toPandas()
    
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

    def get_spark_dataframe(self, paths):
        '''spark_dataframe 생성'''
        df = self.spark.createDataFrame([(path,) for path in paths], ["full_path"])
        df = df.withColumn("file_id", self.extract_file_id("full_path"))
        df = df.withColumn("file_name", self.extract_file_name("full_path"))
        df = df.withColumn("folder_name", self.extract_folder_name("full_path"))
        df = df.withColumn("file_size", self.extract_file_size("full_path"))
        return df

    # def check_data_type(self, df):
    #     jpg_counts = df.filter(col("file_name").contains(".jpg")).count()
    #     png_counts = df.filter(col("file_name").contains(".png")).count()
    #     jpeg_counts = df.filter(col("file_name").contains(".jpeg")).count()
    #     csv_counts = df.filter(col("file_name").contains(".csv")).count()
    #     json_counts = df.filter(col("file_name").contains(".json")).count()
    #     etc_counts = df.filter(~col("file_name").rlike(r"\.(jpg|png|jpeg|csv|json)$")).count()

    #     type_list = [('jpg_counts', jpg_counts), ('jpeg_counts', jpeg_counts), 
    #                  ('png_counts', png_counts), ('csv_counts', csv_counts), ('json_counts', json_counts), ('etc_counts', etc_counts)]
    #     result_df = self.spark.createDataFrame(type_list)
    #     return result_df
    
    def read_json(self, path):
        return self.spark.read.json(path, multiLine=True)

    def read_parquet(self, path):
        return self.spark.read.parquet(path)
        
    def spark_stop(self):
        return self.spark.stop()
    # def __del__(self):
    #     # 클래스 인스턴스가 소멸될 때 SparkSession을 종료
    #     self.spark.stop()



