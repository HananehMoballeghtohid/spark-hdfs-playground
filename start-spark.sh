#!/usr/bin/env bash

echo "Starting Spark services..."
docker-compose -f docker-compose-ha.yml up -d spark-master

# Wait for Spark master to be ready
echo "Waiting for Spark Master to be ready..."
sleep 10

# Copy Hadoop configs to Spark containers after they start
echo "Configuring Spark containers with Hadoop settings..."
for container in spark-master; do
    docker exec $container cp /opt/hadoop-config/core-site.xml /opt/bitnami/spark/conf/
    docker exec $container cp /opt/hadoop-config/hdfs-site.xml /opt/bitnami/spark/conf/
    docker exec $container cp /opt/hadoop-config/yarn-site.xml /opt/bitnami/spark/conf/
done

# Start Spark workers
docker-compose -f docker-compose-ha.yml up -d spark-worker1 spark-worker2 spark-worker3

# Configure workers with Hadoop settings
sleep 5
for container in spark-worker1 spark-worker2 spark-worker3; do
    docker exec $container cp /opt/hadoop-config/core-site.xml /opt/bitnami/spark/conf/
    docker exec $container cp /opt/hadoop-config/hdfs-site.xml /opt/bitnami/spark/conf/
    docker exec $container cp /opt/hadoop-config/yarn-site.xml /opt/bitnami/spark/conf/
done

# Start Spark History Server and Jupyter
docker-compose -f docker-compose-ha.yml up -d spark-history-server jupyter-spark

echo "Hadoop + Spark HA Cluster initialization complete!"
echo ""
echo "=== Service URLs ==="
echo "HDFS:"
echo "  - NameNode1 UI: http://localhost:9870"
echo "  - NameNode2 UI: http://localhost:9871"
echo ""
echo "YARN:"
echo "  - ResourceManager1 UI: http://localhost:8088"
echo "  - ResourceManager2 UI: http://localhost:8089"
echo ""
echo "Spark:"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Spark History Server: http://localhost:18080"
echo "  - Jupyter Notebook: http://localhost:8888 (token: spark123)"
echo ""
echo "To verify cluster status:"
echo "  docker-compose -f docker-compose-ha.yml ps"
echo ""
echo "To check NameNode HA status:"
echo "  docker-compose -f docker-compose-ha.yml exec namenode1 hdfs haadmin -getServiceState nn1"
echo "  docker-compose -f docker-compose-ha.yml exec namenode1 hdfs haadmin -getServiceState nn2"


