#!/usr/bin/env bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting NYC Taxi Data Upload to HDFS${NC}"
echo "================================================"

# Check if cluster is running
if ! docker ps | grep -q namenode1; then
    echo -e "${RED}Error: Hadoop cluster is not running. Please start it first.${NC}"
    exit 1
fi

# Create a temporary directory for downloads
TEMP_DIR="./temp-taxi-data"
mkdir -p $TEMP_DIR

# Download files
echo -e "${YELLOW}Downloading taxi zone lookup CSV...${NC}"
wget -q --show-progress -O $TEMP_DIR/taxi_zone_lookup.csv \
    https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to download taxi_zone_lookup.csv${NC}"
    exit 1
fi

echo -e "${YELLOW}Downloading yellow tripdata parquet file (June 2024)...${NC}"
wget -q --show-progress -O $TEMP_DIR/yellow_tripdata_2024-06.parquet \
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-06.parquet

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to download yellow_tripdata_2024-06.parquet${NC}"
    exit 1
fi

echo -e "${GREEN}Downloads completed successfully!${NC}"

# Wait for HDFS to be ready
echo -e "${YELLOW}Waiting for HDFS to be ready...${NC}"
docker exec namenode1 hdfs dfsadmin -safemode wait > /dev/null 2>&1

# Create HDFS directory structure
echo -e "${YELLOW}Creating HDFS directory /data/taxi/...${NC}"
docker exec namenode1 hdfs dfs -mkdir -p /data/taxi

# Copy files to namenode1 container first
echo -e "${YELLOW}Copying files to namenode1 container...${NC}"
docker cp $TEMP_DIR/taxi_zone_lookup.csv namenode1:/tmp/
docker cp $TEMP_DIR/yellow_tripdata_2024-06.parquet namenode1:/tmp/

# Upload files to HDFS
echo -e "${YELLOW}Uploading taxi_zone_lookup.csv to HDFS...${NC}"
docker exec namenode1 hdfs dfs -put -f /tmp/taxi_zone_lookup.csv /data/taxi/

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ taxi_zone_lookup.csv uploaded successfully${NC}"
else
    echo -e "${RED}✗ Failed to upload taxi_zone_lookup.csv${NC}"
fi

echo -e "${YELLOW}Uploading yellow_tripdata_2024-06.parquet to HDFS...${NC}"
docker exec namenode1 hdfs dfs -put -f /tmp/yellow_tripdata_2024-06.parquet /data/taxi/

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ yellow_tripdata_2024-06.parquet uploaded successfully${NC}"
else
    echo -e "${RED}✗ Failed to upload yellow_tripdata_2024-06.parquet${NC}"
fi

# Set permissions
echo -e "${YELLOW}Setting HDFS permissions...${NC}"
docker exec namenode1 hdfs dfs -chmod -R 755 /data
docker exec namenode1 hdfs dfs -chown -R spark:spark /data/taxi

# Verify uploads
echo -e "${YELLOW}Verifying uploaded files...${NC}"
echo -e "${GREEN}HDFS Directory listing:${NC}"
docker exec namenode1 hdfs dfs -ls -h /data/taxi/

# Show file sizes
echo -e "
${GREEN}File sizes in HDFS:${NC}"
docker exec namenode1 hdfs dfs -du -h /data/taxi/

# Clean up temporary files
echo -e "${YELLOW}Cleaning up temporary files...${NC}"
docker exec namenode1 rm -f /tmp/taxi_zone_lookup.csv /tmp/yellow_tripdata_2024-06.parquet
rm -rf $TEMP_DIR

echo -e "
${GREEN}✓ NYC Taxi data successfully uploaded to HDFS!${NC}"
echo -e "${GREEN}Files are available at:${NC}"
echo "  - hdfs:///data/taxi/taxi_zone_lookup.csv"
echo "  - hdfs:///data/taxi/yellow_tripdata_2024-06.parquet"

