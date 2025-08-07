package com.computer_technology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.computer_technology.Session.GetSpark;
import static org.apache.spark.sql.functions.*;

public class LongTrips {
    public static void main(String[] args) {
        try(SparkSession spark = GetSpark()) {
            Dataset<Row> yellowTrips = Session.yellowTrips(spark);

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
                    .parquet("hdfs://namenode1:9000/output/q1_long_trips.parquet");

            spark.stop();
        }
    }
}

