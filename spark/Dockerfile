FROM openjdk:8-jdk-slim

ARG SPARK_VERSION=3.2.1
ARG HADOOP_VERSION=3.2.0
ARG HADOOP_VERSION_SPARK=3.2
ENV SPARK_HOME=/opt/spark

# Install wget and fix SSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SPARK}.tgz -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SPARK} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# Download required Hadoop jars
RUN for jar in hadoop-client-api hadoop-client-runtime hadoop-common hadoop-hdfs hadoop-hdfs-client hadoop-auth; do \
    wget --timeout=30 --tries=5 https://repo1.maven.org/maven2/org/apache/hadoop/$jar/${HADOOP_VERSION}/$jar-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/ ; \
    done

WORKDIR /app

RUN mkdir -p /etc/hadoop

COPY ../config/hdfs-site.xml /etc/hadoop/hdfs-site.xml
COPY ../config/core-site.xml /etc/hadoop/core-site.xml

ENV HADOOP_CONF_DIR=/etc/hadoop
ENV SPARK_CONF_DIR=$SPARK_HOME/conf

# This command keeps the container running
CMD ["tail", "-f", "/dev/null"]