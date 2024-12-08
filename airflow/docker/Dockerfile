# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Sử dụng base image Airflow
FROM apache/airflow:2.10.3

# Chạy lệnh dưới quyền root để cài đặt các công cụ cần thiết
USER root

# Cập nhật hệ thống và cài đặt các thư viện cần thiết
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    openjdk-17-jdk \
    procps \
    && apt-get clean

ENV SPARK_VERSION=3.3.1
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
RUN pip install apache-airflow-providers-celery

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt
