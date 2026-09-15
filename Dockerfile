FROM python:3.11-slim

# Java is required by PySpark under the hood (Spark runs on the JVM).
# It's installed here, inside the image — your host machine never needs it.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash"]
