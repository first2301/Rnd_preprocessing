FROM jupyter/pyspark-notebook:latest

USER root

# Install Java Development Kit (JDK)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Install Pydoop
RUN pip install pydoop

USER jovyan

# # 베이스 이미지로 Python 3.9 사용
# FROM python:3.9-slim

# # 작업 디렉토리 설정
# WORKDIR /app

# # 필요한 패키지를 미리 설치 (필요에 따라 수정 가능)
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # 현재 디렉토리의 모든 파일을 컨테이너의 /app 디렉토리에 복사
# COPY . .

# # 컨테이너 시작 시 실행할 명령어 (필요에 따라 수정 가능)
# CMD ["python", "app.py"]


# FROM ubuntu:22.04

# # 환경 변수 설정
# ENV DEBIAN_FRONTEND=noninteractive

# # 기본 패키지 설치 및 업데이트
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#         software-properties-common \
#         && add-apt-repository ppa:deadsnakes/ppa \
#         && apt-get update \
#         && apt-get install -y --no-install-recommends \
#         python3.10 \
#         python3.10-dev \
#         python3.10-distutils \
#         build-essential \
#         git \
#         vim \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# # Python 패키지 관리를 위한 pip 업그레이드 및 설정
# RUN curl -sSL https://bootstrap.pypa.io/get-pip.py | python3.10

# # 작업 디렉토리 설정
# WORKDIR /app

# # 필요한 Python 패키지 설치 (requirements.txt 파일 필요)
# COPY requirements.txt .
# RUN python3.10 -m pip install --no-cache-dir -r requirements.txt

# # 현재 디렉토리의 모든 파일을 컨테이너의 /app 디렉토리에 복사
# COPY . .

# # 컨테이너 시작 시 실행할 명령어 (필요에 따라 수정 가능)
# CMD ["python", "app.py"]
