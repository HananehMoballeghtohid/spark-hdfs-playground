package com.computer_technology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Session {
    public static Dataset<Row> yellowTrips(SparkSession spark) {
        return spark.read()
                .parquet("hdfs://namenode1:9000/data/taxi/yellow_tripdata_2024-06.parquet");
    }

    public static Dataset<Row> taxiZones(SparkSession spark) {
        return spark.read()
                .option("header", "true")
                .csv("hdfs://namenode1:9000/data/taxi/taxi_zone_lookup.csv");

    }

    public static SparkSession GetSpark() {
        return SparkSession.builder()
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
    }
}
