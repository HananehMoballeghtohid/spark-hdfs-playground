# Build stage
FROM maven:3.8.4-openjdk-11-slim AS build

WORKDIR /app

# Set Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install necessary tools
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Spark (minimal - just what's needed for spark-submit)
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar -xzf spark-3.5.0-bin-hadoop3.tgz \
    && mv spark-3.5.0-bin-hadoop3 /opt/spark \
    && rm spark-3.5.0-bin-hadoop3.tgz

# Copy Maven files first for better caching
COPY pom.xml ./
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn clean package -DskipTests

# Expose Spark UI ports (optional)
EXPOSE 4040 4041 4042

# Default spark-submit command
CMD ["spark-submit", \
     "--class", "com.computer-technology-sharif.SparkHadoopHAApp", \
     "--master", "yarn", \
     "--deploy-mode", "client", \
     "app.jar"]

