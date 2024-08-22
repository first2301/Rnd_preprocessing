import logging

class Database:
    def __init__(self):
        pass

    def table_exists(self, conn, table_name):
        """테이블이 존재하는지 확인"""
        query = f"SHOW TABLES LIKE '{table_name}';"
        result = conn.execute(query).fetchone()
        return len(result) > 0 

    def create_table(self, conn, table_name, df_name):
        """이미 존재하는 테이블 없으면 생성"""

        if self.table_exists(conn, table_name):
            logging.info(f"테이블 {table_name}은 이미 존재합니다.")
            return
        try:
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {df_name}")
            logging.info(f"테이블 {table_name} 생성 완료.")
        except Exception as e:
            logging.error(f"테이블 {table_name} 생성 중 에러 발생: {e}")

