from lib.pl_lib import PolarsDataFrame
from lib.prepro import Preprocessing
import polars as pl
import duckdb

PATH_CCTV_DATA = './exhdd/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
PATH_CCTV_LABEL_DATA = './exhdd/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/라벨링데이터/*'

PATH_SAND_DATA = './exhdd/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
PATH_SAND_LABEL_DATA = './exhdd/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'

# 라이브러리 호출
pdf = PolarsDataFrame()
prepro = Preprocessing()

# 데이터 경로 불러오기
cctv_paths = prepro.get_all_file_paths(PATH_CCTV_DATA)
cctv_label_paths = prepro.get_all_file_paths(PATH_CCTV_LABEL_DATA)
sand_paths = prepro.get_all_file_paths(PATH_SAND_DATA)
sand_label_paths = prepro.get_all_file_paths(PATH_SAND_LABEL_DATA)

# polars dataframe 생성
cctv_df = pdf.get_polars_dataframe(cctv_paths)
cctv_label_df = pdf.get_polars_dataframe(cctv_label_paths)
sand_df = pdf.get_polars_dataframe(sand_paths)
sand_label_df = pdf.get_polars_dataframe(sand_label_paths)

# duckdb 생성 및 연결
conn = duckdb.connect("./exhdd/industry_data/preprocessed_data/industry.duckdb")

# duckdb에 polars dataframe 저장 
conn.execute("CREATE TABLE cctv_data AS SELECT * FROM cctv_df")
conn.execute("CREATE TABLE cctv_label_data AS SELECT * FROM cctv_label_df")
conn.execute("CREATE TABLE sand_data AS SELECT * FROM sand_df")
conn.execute("CREATE TABLE sand_label_data AS SELECT * FROM sand_label_df")