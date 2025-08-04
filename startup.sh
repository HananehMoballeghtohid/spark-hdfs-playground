#!/usr/bin/env bash

set -e

echo "Creating necessary directories..."
mkdir -p ./hadoop-config
mkdir -p ./spark-config

echo "Setting correct permissions..."
chmod -R 777 ./spark-config

echo "Starting ZooKeeper cluster..."
docker-compose up -d zookeeper1 zookeeper2 zookeeper3
echo "Waiting for ZooKeeper to stabilize..."
sleep 30

echo "Starting JournalNodes..."
docker-compose up -d journalnode1 journalnode2 journalnode3
echo "Waiting for JournalNodes to be ready..."
sleep 20

echo "Starting primary NameNode..."
docker-compose up -d namenode1
sleep 10

# Only format if not already formatted
echo "Checking if HDFS needs formatting..."
docker-compose exec -T namenode1 hdfs namenode -format -clusterId mycluster -force 2>/dev/null || echo "HDFS already formatted or format failed"

echo "Initializing HA in ZooKeeper..."
docker-compose exec -T namenode1 hdfs zkfc -formatZK -force 2>/dev/null || echo "ZK already initialized"

echo "Starting standby NameNode..."
docker-compose up -d namenode2
sleep 10

echo "Bootstrapping standby NameNode..."
docker-compose exec -T namenode2 hdfs namenode -bootstrapStandby -force 2>/dev/null || echo "Standby already bootstrapped"

echo "Starting ZKFC controllers..."
docker-compose up -d zkfc1 zkfc2
sleep 10

echo "Starting DataNodes..."
docker-compose up -d datanode1 datanode2 datanode3
sleep 20

echo "Starting YARN ResourceManagers..."
docker-compose up -d resourcemanager1 resourcemanager2
sleep 20

echo "Starting NodeManager..."
docker-compose up -d nodemanager1
sleep 10

echo "Creating HDFS directories for Spark..."
docker-compose exec -T namenode1 hdfs dfs -mkdir -p /spark-logs 2>/dev/null || true
docker-compose exec -T namenode1 hdfs dfs -mkdir -p /spark-warehouse 2>/dev/null || true
docker-compose exec -T namenode1 hdfs dfs -chmod 777 /spark-logs 2>/dev/null || true
docker-compose exec -T namenode1 hdfs dfs -chmod 777 /spark-warehouse 2>/dev/null || true

echo "Starting Spark cluster..."
docker-compose up -d spark-master
sleep 10
docker-compose up -d spark-worker-1 spark-worker-2
sleep 5
docker-compose up -d spark-history-server

echo "========================================="
echo "Cluster started successfully!"
echo "========================================="
echo "HDFS UI (NameNode1): http://localhost:9870"
echo "HDFS UI (NameNode2): http://localhost:9871"
echo "YARN UI (RM1): http://localhost:8088"
echo "YARN UI (RM2): http://localhost:8089"
echo "Spark Master UI: http://localhost:8080"
echo "Spark History Server: http://localhost:18080"
echo "========================================="

# Show cluster status
echo ""
echo "Checking HDFS status..."
docker-compose exec -T namenode1 hdfs dfsadmin -report || echo "HDFS not ready yet"
