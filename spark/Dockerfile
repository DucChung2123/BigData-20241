FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark pandas

COPY ./jars/postgresql-42.7.4.jar /opt/spark/jars/

WORKDIR /opt/spark/app

# CMD ["/opt/bitnami/scripts/spark/entrypoint.sh"]


