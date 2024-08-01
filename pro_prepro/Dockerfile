FROM python:3.10
# FROM ubunut:22.04
    
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk
# Upgrade pip and setuptools
RUN pip install --upgrade pip && \
    pip install --upgrade pip setuptools 

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64


WORKDIR /app

COPY . .

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]