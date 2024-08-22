import os
# import json
import time
import glob
import psutil
import sqlite3
import logging
import zipfile
import subprocess
from tqdm import tqdm
from pathlib import Path
import polars as pl


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

    def check_total_dir(self):
        current_directory = Path.cwd() 
        # tree 명령어 실행 (현재 디렉토리만 포함)
        total_dir = subprocess.run(['tree', '-d', current_directory], text=True, capture_output=True, check=True)
        # 실행 결과를 로그 파일에 저장
        if total_dir.returncode == 0:
            logging.info("전체 디렉토리 구조:\n%s", total_dir.stdout)
