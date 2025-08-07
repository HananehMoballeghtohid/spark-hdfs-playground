# spark-hdfs-playground
This project sets up a high-availability HDFS cluster using Docker Compose, integrates Spark for distributed processing, and processes NYC taxi trip data via Java-based Spark applications.
## Project Components
- HDFS Cluster with HA: NameNodes, JournalNodes, DataNodes, ZooKeeper
- YARN: ResourceManagers and NodeManagers
- Spark Cluster: Master and workers with History Server
- Java Spark App Container: For submitting Spark jobs written in Java
## Project Setup and Execution Guide
### Quick Start
Follow these steps to set up and run the project:
#### 1. Start HDFS + Spark cluster
Start all the necessary containers:
```bash
./startup.sh
```
You can access HDFS Web UI at http://localhost:9870
### 2. Upload the NYC Taxi Dataset to HDFS
```bash
./upload-taxi-data.sh
```
This script will:
- Download the raw datasets
- Upload the datasets to HDFS under a predefined path
### 3. Submit a Java Spark Job
``` bash
./submit.sh [job-name]
```
Replace [job-name] with the name of your job class. This script:
- Submits a Java-based Spark job to the cluster
- Executes the Spark job
- Writes output back to HDFS

> **_NOTE:_**  Make sure all scripts have execute permissions ```chmod +x *.sh``` before running them.


- Google drive links to the videos: https://drive.google.com/drive/folders/1vSDA0EVIccuH5iL9y3fSHCK3kTtXEU-O?usp=sharing
- GitHub link: https://github.com/HananehMoballeghtohid/spark-hdfs-playground
