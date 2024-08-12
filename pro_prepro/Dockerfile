# jupyter image load
FROM jupyter/pyspark-notebook:latest

# root 전환
USER root

# 권한 부여
RUN chown -R jovyan /home/jovyan/work
RUN chmod -R 777 /home/jovyan/work

USER jovyan

# Copy jdbc driver jar file 
COPY ./driver/postgresql-42.7.3.jar /usr/local/spark/jars/
COPY ./driver/sqlite-jdbc-3.46.0.1.jar /usr/local/spark/jars/

# add dependency 
RUN pip install --no-cache-dir findspark \ 
                                pyarrow \
                                psycopg2-binary \
                                pyspark

# # jovyan 그룹이 존재하지 않는 경우 그룹 추가
# RUN groupadd -r jovyan || true
# # jovyan 사용자 및 그룹 추가 (사용자가 이미 존재하는 경우에는 스킵)
# RUN useradd -r -g jovyan jovyan || true

# RUN mkdir -p /home/jovyan/work/local_src/preprocessed_data

# # 디렉토리와 파일의 소유권 변경
# RUN chown -R jovyan:jovyan /home/jovyan/work/local_src/preprocessed_data

# # 권한 설정
# RUN chmod -R 777 /home/jovyan/work/local_src/preprocessed_data

# USER jovyan

# FROM python:3.10
# # FROM ubunut:22.04
    
# RUN apt-get clean && \
#     rm -rf /var/lib/apt/lists/* && \
#     apt-get update && \
#     apt-get install -y openjdk-11-jdk
# # Upgrade pip and setuptools
# RUN pip install --upgrade pip && \
#     pip install --upgrade pip setuptools 

# # Set JAVA_HOME environment variable
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64


# WORKDIR /app

# COPY . .

# COPY requirements.txt .

# RUN pip install --no-cache-dir -r requirements.txt

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]