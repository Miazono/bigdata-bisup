FROM apache/airflow:3.1.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    tar && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# ENV SPARK_VERSION=3.5.1
# ENV SPARK_HOME=/opt/spark
# RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o /tmp/spark.tgz && \
#     tar -xvzf /tmp/spark.tgz -C /opt && \
#     mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
#     rm /tmp/spark.tgz

# ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
