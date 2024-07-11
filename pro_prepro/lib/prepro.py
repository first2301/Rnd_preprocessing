import json
import time
import psutil
import logging
import pandas as pd
from tqdm import tqdm

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