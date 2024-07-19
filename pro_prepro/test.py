
import logging
from lib.prepro import Preprocessing


prepro = Preprocessing() # 데이터 전처리 라이브러리 호출
# logging 모드 설정: logging.INFO / logging.DEBUG
# log_file = './log/test_log.log' # 데이터 처리 로그 저장 경로
# logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 


PATH_CCTV_DATA = 'G:/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
PATH_SAND_DATA = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
PATH_SAND_LABEL = 'G:/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'

cctv_data = prepro.get_all_file_paths(PATH_CCTV_DATA)
# sand_data = prepro.get_all_file_paths(PATH_SAND_DATA)
# sand_label_data = prepro.get_all_file_paths(PATH_SAND_LABEL)

logging.info('test 진행중')
logging.info(PATH_CCTV_DATA)
logging.info(PATH_SAND_DATA)
logging.info(PATH_SAND_LABEL)