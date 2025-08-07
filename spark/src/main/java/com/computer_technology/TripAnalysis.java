package com.computer_technology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class TripAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Trip Duration Analysis")
                .master("spark://spark-master:7077")
                .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-cluster")
                .config("spark.hadoop.dfs.nameservices", "hadoop-cluster")
                .config("spark.hadoop.dfs.ha.namenodes.hadoop-cluster", "nn1,nn2")
                .config("spark.hadoop.dfs.namenode.rpc-address.hadoop-cluster.nn1", "namenode1:9000")
                .config("spark.hadoop.dfs.namenode.rpc-address.hadoop-cluster.nn2", "namenode2:9000")
                .config("spark.hadoop.dfs.client.failover.proxy.provider.hadoop-cluster",
                        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
                .config("spark.eventLog.enabled", "false")
                .getOrCreate();

        // Load Parquet data from HDFS
        Dataset<Row> yellowTrips = spark.read()
                .parquet("hdfs://namenode1:9000/data/taxi/yellow_tripdata_2024-06.parquet");

        // Filter trips and add duration column
        Dataset<Row> filtered = yellowTrips
                .filter(col("passenger_count").gt(2)
                        .and(col("trip_distance").gt(5)))
                .withColumn("duration_minutes",
                        expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)"));

        // Sort by duration_minutes descending
        Dataset<Row> result = filtered.orderBy(col("duration_minutes").desc());

        // Write to Parquet
        result.write()
                .mode("overwrite")
                .parquet("hdfs://namenode1:9000/output/long_trips_analysis");

        spark.stop();
    }
}

