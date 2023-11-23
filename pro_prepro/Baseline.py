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

def format_bytes(size): # 파일 용량 계산
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

def normalize_json(json_meta_data): # json 형태의 데이터를 DataFrame으로 변환
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

def log_system_resources():
    '''
    불필요한 자원이 사용되고 있는지 확인하는 함수
    '''
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

start_time = time.time() # 프로그램 시작 시간 기록

# logging 모드 설정: logging.INFO / logging.DEBUG
log_file = './log/prepro.log' # 데이터 처리 로그 저장 경로
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
logging.info('데이터 처리 시작')
log_system_resources() # 시스템 리소스 확인

data_dir = './data/' 
data_path = subprocess.run(['tree', '-d', data_dir], text=True, capture_output=True, check=True) # 전체 데이터 디렉토리 정보 확인
if data_path.returncode == 0:
    logging.info("데이터 디렉토리 확인:\n%s", data_path.stdout) # 로그 파일에 디렉토리 정보 출력

logging.info('데이터 로딩')
# pd.set_option('display.max_columns', None) # 데이터프레임에서 전체 컬럼 보이기
img_dir_list = glob.glob('./data/image/TS*') # 처리할 데이터 저장 경로
img_total_list = [glob.glob(folder+'/*') for folder in img_dir_list]
img_paths = [img_path for sublist in img_total_list for img_path in sublist]
# img_type_data = [img_paths[n].split('.')[-1] for n in range(len(img_paths))] 

# 이미지 데이터에서 메타 데이터 추출
logging.info('메타 데이터 추출 시작..') # 데이터 처리 및 데이터프레임 생성
df = pd.DataFrame({'file_id': [img_paths[n].split('.')[-2].split('/')[-1] for n in range(len(img_paths))], # 이미지 데이터 경로에서 파일명으로 되어있는 id 분리
                    'full_path': img_paths, # 전체 이미지 데이터 경로 
                    'folder': [img_paths[n].split('.')[-2].split('/')[-2] for n in range(len(img_paths))], # 폴더명 추출
                    'file_name': [path.split('/')[-1] for path in img_paths],
                    'file_size': [os.path.getsize(path) for path in tqdm(img_paths)], # 각각의 파일 크기 
                    }) 
label_data = glob.glob('./data/label_data/*.json') # 메타 데이터 경로
# 메타 데이터 추출 및 데이터프레임 생성
label_df = pd.DataFrame({"file_id": [label.split('.')[-2].split('/')[-1] for label in label_data],
                        "label_paths": label_data,})
logging.info('메타 데이터 추출 완료')

logging.info('테이블 생성 시작..')
json_meta_data = normalize_json(label_df['label_paths']) # json 데이터를 pandas DataFrame로 변경

# json에서 pandas DataFrame로 변환한 여러 데이터를 하나의 DataFrame로 concat해서 종합
concat_df = pd.concat(json_meta_data, ignore_index=True) 

'''
Dataset.quality1: 품질정보1(밀도) / 2.5 이상
Dataset.quality2: 품질정보2(흡수율) / 3% 이하 (흡수율이 높으면 콘크리트 물비 증가 → 강도저하 및 균열 문제 발생)
Dataset.quality3: 품질정보3(안전성) / 5% 이하 (외부 화학적, 기상적 환경영향성 → 내구성 문제 발생)
Dataset.quality4: 품질정보4(잔입자 통과량) / 잔입자량이 많으면 콘크리트 물비 증가 → 강도저하 및 균열 문제 발생)
'''
new_df = concat_df[['id', 'image.size.width', 'image.size.height', 'dataset.quality1', 'dataset.quality2', 'dataset.quality3', 'dataset.quality4', 'dataset.result']]
new_df.columns = ['file_id', 'width', 'height', '밀도', '흡수율', '안전성', '잔입자_통과량', 'target_data']
merge_df = pd.merge(df, new_df, on='file_id') # 이미지에서 추출한 메타 데이터와 json에서 DataFrame으로 변환한 데이터를 inner join

updated_df = merge_df.drop('file_id', axis=1)
# updated_df.set_index(merge_df['file_id']) # test

logging.info('데이터 테이블 생성 완료') 
logging.info(f"Table row: {len(updated_df)}, Table columns: {len(updated_df.columns)}")

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
            전체 이미지 데이터 용량: {format_bytes(file_size)}
            png counts: {png_num} 
            jpg counts: {jpg_num}
            etc counts: {etc}
            --------------------------------------
            """
logging.info(file_info) # 데이터 처리 정보 확인

# 데이터베이스 생성 및 저장
db_path = './database/database.db'
conn = sqlite3.connect(db_path)
logging.info('데이터베이스 생성')

if Path(db_path).exists():
    logging.info("데이터베이스 경로: %s", db_path)

# 데이터 베이스에 데이터 저장
updated_df.to_sql('TB_meta_info', conn, if_exists='replace', index=False)
logging.info('데이터베이스에 데이터 저장 완료')

conn.close() 
logging.info('데이터베이스 연결 종료')

# 전체 디렉토리 확인
current_directory = Path.cwd() 
# tree 명령어 실행 (현재 디렉토리만 포함)
total_dir = subprocess.run(['tree', '-d', current_directory], text=True, capture_output=True, check=True)
# 실행 결과를 로그 파일에 저장
if total_dir.returncode == 0:
    logging.info("전체 디렉토리 구조:\n%s", total_dir.stdout)

end_time = time.time()  # 실행 후 시간을 기록
elapsed_time = end_time - start_time  # 경과된 시간 계산
minutes, seconds = divmod(elapsed_time, 60) # ms를 분, 초로 변환
logging.info("경과 시간: {}분 {}초".format(int(minutes), int(seconds))) # 분, 초로 변환한 데이터 로깅
logging.info('데이터 처리 종료') 