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


class Preprocessing:
    def __init__(self):
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

    def normalize_json(self, json_meta_data): # json 형태의 데이터를 DataFrame으로 변환
        '''
        json데이터를 pandas DataFrame로 변경하는 함수
        '''
        json_df = []
        for file in tqdm(json_meta_data):
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                data_df = pd.json_normalize(data)
                json_df.append(data_df)
        return json_df

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

    def get_file_name(path):
        return os.path.basename(path)
    
    def get_file_id(path):
        file_name = os.path.basename(path)
        file_id = os.path.splitext(file_name)[0]
        return file_id
        # return file_name.split('.')[0] 
        # return os.path.splitext(file_name)[0]
        
    def get_folder_name(path):
        return os.path.basename(os.path.dirname(path))
    
    def get_file_size(path):
        return os.path.getsize(path)
    
    def ptint_data_info(self, merge_df):
        # 데이터 타입별로 counts 확인
        img_types = ['png', 'jpg', 'jpeg', 'etc']
        png_num = 0
        jpg_num = 0
        etc = 0
        img_type_data = [] 
        for data in merge_df['full_path']:
            img_type_data.append(data.split('.')[-1])
        
        for idx, img_types in enumerate(img_type_data):
            if img_types == 'png':
                png_num += 1
            elif img_types == 'jpg' or img_types == 'jpeg':
                jpg_num += 1
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
