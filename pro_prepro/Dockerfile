FROM jupyter/pyspark-notebook:latest

# JDBC 드라이버 JAR 파일 추가
COPY ./driver/postgresql-42.7.3.jar /usr/local/spark/jars/
COPY ./driver/sqlite-jdbc-3.46.0.1.jar /usr/local/spark/jars/

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