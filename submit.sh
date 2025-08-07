#!/usr/bin/env bash

read -p "Enter classname: " classname

cd spark
mvn clean package
docker cp ./target/spark-hadoop-ha-app-1.0-SNAPSHOT.jar spark-master:/opt/job-$classname.jar
docker exec spark-master spark-submit \
  --class $classname \
  --deploy-mode client \
  /opt/job-$classname.jar

cd ../
