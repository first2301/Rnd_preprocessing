from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import polars as pl
import duckdb
from lib.pl_lib import PolarsDataFrame
from lib.prepro import Preprocessing
from datetime import datetime

# 상수 정의
PATH_CCTV_DATA = './exhdd/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
PATH_CCTV_LABEL_DATA = './exhdd/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/라벨링데이터/*'
PATH_SAND_DATA = './exhdd/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
PATH_SAND_LABEL_DATA = './exhdd/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'

# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 20),
    'retries': 1,
}

dag = DAG(
    'data_pipeline_test',
    default_args=default_args,
    description='테스트용 ETL 데이터 파이프라인',
    schedule='@once',  # 한 번만 실행
    start_date=datetime(2024, 8, 20),  # 테스트 시작일
    end_date=datetime(2024, 8, 20),  # 테스트 종료일 (단 한 번 실행됨)
    catchup=False,
)

def get_file_paths():
    prepro = Preprocessing()
    return {
        'cctv_paths': prepro.get_all_file_paths(PATH_CCTV_DATA),
        'cctv_label_paths': prepro.get_all_file_paths(PATH_CCTV_LABEL_DATA),
        'sand_paths': prepro.get_all_file_paths(PATH_SAND_DATA),
        'sand_label_paths': prepro.get_all_file_paths(PATH_SAND_LABEL_DATA)
    }

def create_polars_dataframes(**kwargs):
    ti = kwargs['ti']
    file_paths = ti.xcom_pull(task_ids='get_file_paths')
    pdf = PolarsDataFrame()
    return {
        'cctv_df': pdf.get_polars_dataframe(file_paths['cctv_paths']),
        'cctv_label_df': pdf.get_polars_dataframe(file_paths['cctv_label_paths']),
        'sand_df': pdf.get_polars_dataframe(file_paths['sand_paths']),
        'sand_label_df': pdf.get_polars_dataframe(file_paths['sand_label_paths'])
    }

def save_to_duckdb(**kwargs):
    ti = kwargs['ti']
    dataframes = ti.xcom_pull(task_ids='create_polars_dataframes')
    conn = duckdb.connect("./exhdd/industry_data/preprocessed_data/industry.duckdb")
    conn.execute("CREATE TABLE IF NOT EXISTS cctv_data AS SELECT * FROM cctv_df")
    conn.execute("CREATE TABLE IF NOT EXISTS cctv_label_data AS SELECT * FROM cctv_label_df")
    conn.execute("CREATE TABLE IF NOT EXISTS sand_data AS SELECT * FROM sand_df")
    conn.execute("CREATE TABLE IF NOT EXISTS sand_label_data AS SELECT * FROM sand_label_df")

# Airflow 태스크 정의
get_file_paths_task = PythonOperator(
    task_id='get_file_paths',
    python_callable=get_file_paths,
    dag=dag,
)

create_polars_dataframes_task = PythonOperator(
    task_id='create_polars_dataframes',
    python_callable=create_polars_dataframes,
    dag=dag,
)

save_to_duckdb_task = PythonOperator(
    task_id='save_to_duckdb',
    python_callable=save_to_duckdb,
    dag=dag,
)

# 태스크 의존성 설정
get_file_paths_task >> create_polars_dataframes_task >> save_to_duckdb_task



# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
# import polars as pl
# import duckdb
# from lib.pl_lib import PolarsDataFrame
# from lib.prepro import Preprocessing
# from datetime import datetime, timedelta

# # 상수 정의
# PATH_CCTV_DATA = './exhdd/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
# PATH_CCTV_LABEL_DATA = './exhdd/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/라벨링데이터/*'
# PATH_SAND_DATA = './exhdd/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
# PATH_SAND_LABEL_DATA = './exhdd/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'

# # Airflow DAG 정의
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 8, 20), # days_ago(1),
#     'retries': 1,
# }

# dag = DAG(
#     'data_pipeline_test',
#     default_args=default_args,
#     description='테스트용 ETL 데이터 파이프라인',
#     schedule='@once',  # 한 번만 실행
#     start_date=datetime(2024, 8, 20),  # 테스트 시작일
#     end_date=datetime(2024, 8, 20),  # 테스트 종료일 (단 한 번 실행됨)
#     catchup=False,
# )

# # dag = DAG(
# #     'data_pipeline',
# #     default_args=default_args,
# #     description='ETL 데이터 파이프라인',
# #     schedule_interval='@daily',  # 매일 실행
# # )

# # 데이터 경로 불러오기
# def get_file_paths():
#     prepro = Preprocessing()
#     return {
#         'cctv_paths': prepro.get_all_file_paths(PATH_CCTV_DATA),
#         'cctv_label_paths': prepro.get_all_file_paths(PATH_CCTV_LABEL_DATA),
#         'sand_paths': prepro.get_all_file_paths(PATH_SAND_DATA),
#         'sand_label_paths': prepro.get_all_file_paths(PATH_SAND_LABEL_DATA)
#     }

# # 데이터 프레임 생성
# def create_polars_dataframes(file_paths):
#     pdf = PolarsDataFrame()
#     return {
#         'cctv_df': pdf.get_polars_dataframe(file_paths['cctv_paths']),
#         'cctv_label_df': pdf.get_polars_dataframe(file_paths['cctv_label_paths']),
#         'sand_df': pdf.get_polars_dataframe(file_paths['sand_paths']),
#         'sand_label_df': pdf.get_polars_dataframe(file_paths['sand_label_paths'])
#     }

# # DuckDB에 데이터 저장
# def save_to_duckdb(dataframes):
#     conn = duckdb.connect("./exhdd/industry_data/preprocessed_data/industry.duckdb")
    
#     conn.execute("CREATE TABLE IF NOT EXISTS cctv_data AS SELECT * FROM cctv_df")
#     conn.execute("CREATE TABLE IF NOT EXISTS cctv_label_data AS SELECT * FROM cctv_label_df")
#     conn.execute("CREATE TABLE IF NOT EXISTS sand_data AS SELECT * FROM sand_df")
#     conn.execute("CREATE TABLE IF NOT EXISTS sand_label_data AS SELECT * FROM sand_label_df")

# # Airflow 태스크 정의
# get_file_paths_task = PythonOperator(
#     task_id='get_file_paths',
#     python_callable=get_file_paths,
#     dag=dag,
# )

# create_polars_dataframes_task = PythonOperator(
#     task_id='create_polars_dataframes',
#     python_callable=lambda: create_polars_dataframes(get_file_paths_task.execute({})),
#     dag=dag,
# )

# save_to_duckdb_task = PythonOperator(
#     task_id='save_to_duckdb',
#     python_callable=lambda: save_to_duckdb(create_polars_dataframes_task.execute({})),
#     dag=dag,
# )

# # 태스크 의존성 설정
# get_file_paths_task >> create_polars_dataframes_task >> save_to_duckdb_task
