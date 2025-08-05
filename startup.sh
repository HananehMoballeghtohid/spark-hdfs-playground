#!/usr/bin/env bash

echo "Starting Hadoop HA Cluster initialization..."

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

# Start remaining services
echo "Starting remaining services..."
docker-compose -f docker-compose-ha.yml up -d

echo "Hadoop HA Cluster initialization complete!"
echo "Access NameNode1 UI at: http://localhost:9870"
echo "Access NameNode2 UI at: http://localhost:9871"
echo "Access ResourceManager1 UI at: http://localhost:8088"
echo "Access ResourceManager2 UI at: http://localhost:8089"

