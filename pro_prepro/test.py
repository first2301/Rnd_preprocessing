import time
import logging

from pyspark.sql.session import SparkSession
from pyspark.sql.context import SparkContext
from lib.prepro import Preprocessing

start_time = time.time()

# logging 모드 설정: logging.INFO / logging.DEBUG
log_file = './log/test_log.log' # 데이터 처리 로그 저장 경로
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 

PATH_CCTV_DATA = 'G:/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
PATH_SAND_DATA = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
PATH_SAND_LABEL = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'


prepro = Preprocessing() # 데이터 전처리 라이브러리 호출
cctv_data = prepro.get_all_file_paths(PATH_CCTV_DATA)
sand_data = prepro.get_all_file_paths(PATH_SAND_DATA)
sand_label_data = prepro.get_all_file_paths(PATH_SAND_LABEL)



# logging.info('test 진행중')
print(cctv_data[:1])

# logging.info(PATH_SAND_DATA)

end_time = time.time()  # 실행 후 시간을 기록
elapsed_time = end_time - start_time  # 경과된 시간 계산
minutes, seconds = divmod(elapsed_time, 60) # ms를 분, 초로 변환
logging.info("경과 시간: {}분 {}초".format(int(minutes), int(seconds))) # 분, 초로 변환한 데이터 로깅
logging.info('데이터 처리 종료') 
