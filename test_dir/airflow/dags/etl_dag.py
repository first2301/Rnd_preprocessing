from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="etl_dag",
    start_date=datetime(2024, 8, 22),
    # schedule="@daily"
)
def etl_dag():
    @task
    def extract_data():
        from lib.prepro import Preprocessing
        prepro = Preprocessing()
        PATH_CCTV_DATA = '../data/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/원천데이터/*'
        PATH_CCTV_LABEL_DATA = '../data/industry_data/117.산업시설 열화상 CCTV 데이터/01.데이터/1.Training/라벨링데이터/*'

        PATH_SAND_DATA = '../data/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/01.원천데이터/*'
        PATH_SAND_LABEL_DATA = '../data/industry_data/264.건설 모래 품질 관리데이터/01-1.정식개방데이터/Training/02.라벨링데이터/*'
        # # extract file path 
        cctv_paths = prepro.get_all_file_paths(PATH_CCTV_DATA)
        cctv_label_paths = prepro.get_all_file_paths(PATH_CCTV_LABEL_DATA)

        sand_paths = prepro.get_all_file_paths(PATH_SAND_DATA)
        sand_label_paths = prepro.get_all_file_paths(PATH_SAND_LABEL_DATA)

        return{
            "cctv_paths": cctv_paths, "cctv_label_paths": cctv_label_paths,
            "sand_paths": sand_paths, "sand_label_paths": sand_label_paths
        }

    @task
    def trasform_data(cctv_paths, cctv_label_paths, sand_paths, sand_label_paths):
        from lib.pl_lib import PolarsDataFrame
        pdf = PolarsDataFrame()
        cctv_df = pdf.get_polars_dataframe(cctv_paths)
        cctv_label_df = pdf.get_polars_dataframe(cctv_label_paths)
        sand_df = pdf.get_polars_dataframe(sand_paths)
        sand_label_df = pdf.get_polars_dataframe(sand_label_paths)

        return {
            "cctv_df": cctv_df, "cctv_label_df": cctv_label_df,
            "sand_df": sand_df, "sand_label_df": sand_label_df
        }

    @task
    def load_data(cctv_df, cctv_label_df, sand_df, sand_label_df):
        import duckdb
        from lib.db_lib import Database
        
        db = Database()
        conn = duckdb.connect("")
        
        tables = {
            "cctv_data": "cctv_df",
            "cctv_label_data": "cctv_label_df",
            "sand_data": "sand_df",
            "sand_label_data": "sand_label_df"
        }
        
        for table_name, df_name in tables.items():
            db.create_table(conn, table_name, df_name)

        pass

    extract_task = extract_data()
    transform_task = trasform_data(
        extract_task['cctv_paths'], extract_task['cctv_label_paths'],
        extract_task['sand_paths'], extract_task['sand_label_paths']
    )
    load_task = load_data(
        transform_task['cctv_df'], transform_task['cctv_label_df'],
        transform_task['sand_df'], transform_task['sand_label_df']
    )

etl_pipe = etl_dag()