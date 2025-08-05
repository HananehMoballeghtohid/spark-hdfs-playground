#!/usr/bin/env bash

echo "Starting Hadoop + Spark HA Cluster initialization..."


# Start Zookeeper ensemble first
echo "Starting Zookeeper ensemble..."
docker-compose -f docker-compose-ha.yml up -d zookeeper1 zookeeper2 zookeeper3

# Wait for Zookeeper to be ready
echo "Waiting for Zookeeper to be ready..."
sleep 10

# Start JournalNodes
echo "Starting JournalNodes..."
docker-compose -f docker-compose-ha.yml up -d journalnode1 journalnode2 journalnode3

# Wait for JournalNodes to be ready
echo "Waiting for JournalNodes to be ready..."
sleep 10

# Format the first NameNode
echo "Formatting NameNode1..."
docker-compose -f docker-compose-ha.yml run --rm namenode1 hdfs namenode -format -force

# Start the first NameNode
echo "Starting NameNode1..."
docker-compose -f docker-compose-ha.yml up -d namenode1

# Wait for NameNode1 to be ready
echo "Waiting for NameNode1 to be ready..."
sleep 15

# Bootstrap the standby NameNode
echo "Bootstrapping NameNode2..."
docker-compose -f docker-compose-ha.yml run --rm namenode2 hdfs namenode -bootstrapStandby

# Format ZKFC on both NameNodes
echo "Formatting ZKFC..."
docker-compose -f docker-compose-ha.yml exec namenode1 hdfs zkfc -formatZK -force

# Start the second NameNode
echo "Starting NameNode2..."
docker-compose -f docker-compose-ha.yml up -d namenode2

# Start ZKFC on both namenodes to enable automatic failover
echo "Starting ZKFC daemons..."
docker-compose -f docker-compose-ha.yml exec -d namenode1 hdfs zkfc
docker-compose -f docker-compose-ha.yml exec -d namenode2 hdfs zkfc

# Wait for ZKFC to elect active namenode
echo "Waiting for ZKFC to elect active NameNode..."
sleep 10

# Start remaining Hadoop services
echo "Starting remaining Hadoop services..."
docker-compose -f docker-compose-ha.yml up -d datanode1 datanode2 datanode3
docker-compose -f docker-compose-ha.yml up -d resourcemanager1 resourcemanager2
docker-compose -f docker-compose-ha.yml up -d nodemanager1 nodemanager2 nodemanager3
docker-compose -f docker-compose-ha.yml up -d historyserver

# Wait for HDFS to be ready
echo "Waiting for HDFS to be ready..."
sleep 20

# Determine which namenode is active
ACTIVE_NN=""
if docker-compose -f docker-compose-ha.yml exec -T namenode1 hdfs haadmin -getServiceState nn1 2>/dev/null | grep -q "active"; then
    ACTIVE_NN="namenode1"
    echo "NameNode1 is active"
elif docker-compose -f docker-compose-ha.yml exec -T namenode2 hdfs haadmin -getServiceState nn2 2>/dev/null | grep -q "active"; then
    ACTIVE_NN="namenode2"
    echo "NameNode2 is active"
else
    echo "Warning: Could not determine active NameNode, defaulting to namenode1"
    ACTIVE_NN="namenode1"
fi

# Create necessary HDFS directories using the active namenode
echo "Creating HDFS directories for Spark..."
docker-compose -f docker-compose-ha.yml exec -T $ACTIVE_NN hdfs dfs -mkdir -p /spark-logs
docker-compose -f docker-compose-ha.yml exec -T $ACTIVE_NN hdfs dfs -mkdir -p /spark-warehouse
docker-compose -f docker-compose-ha.yml exec -T $ACTIVE_NN hdfs dfs -mkdir -p /spark/jars
docker-compose -f docker-compose-ha.yml exec -T $ACTIVE_NN hdfs dfs -chmod -R 777 /spark-logs
docker-compose -f docker-compose-ha.yml exec -T $ACTIVE_NN hdfs dfs -chmod -R 777 /spark-warehouse
docker-compose -f docker-compose-ha.yml exec -T $ACTIVE_NN hdfs dfs -chmod -R 777 /spark

# Start Spark services
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

