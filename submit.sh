#!/usr/bin/env bash
set -e

if [ -n "$1" ]; then
  classname="$1"
else
  read -p "Enter class name (without package): " classname
fi

mvn -f spark/pom.xml clean package
docker cp spark/target/spark-hadoop-ha-app-1.0-SNAPSHOT.jar spark-master:/opt/job-$classname.jar
docker exec spark-master spark-submit \
  --class com.computer_technology.$classname \
  --deploy-mode client \
  /opt/job-$classname.jar
