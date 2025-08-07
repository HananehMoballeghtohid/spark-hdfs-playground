package com.computer_technology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class AvgFareByZone {
        public static void main(String[] args) {
                try (SparkSession spark = Session.GetSpark()) {
                        Dataset<Row> yellowTrips = Session.yellowTrips(spark);
                        Dataset<Row> taxiZones = Session.taxiZones(spark);

                        Dataset<Row> tripsWithPickupZone = yellowTrips.join(
                                        taxiZones,
                                        yellowTrips.col("PULocationID").equalTo(taxiZones.col("LocationID")),
                                        "inner");

                        // Group by Zone and Borough, then calculate average fare_amount
                        Dataset<Row> zoneFareAverage = tripsWithPickupZone
                                        .groupBy(
                                                        col("Zone"),
                                                        col("Borough"))
                                        .agg(
                                                        avg("fare_amount").alias("average_fare"))
                                        .orderBy(
                                                        col("average_fare").desc());

                        zoneFareAverage.show(50, false);

                        zoneFareAverage.write()
                                        .mode("overwrite")
                                        .parquet("hdfs://namenode1:9000/output/q2_avg_fare_by_zone.parquet");
                        spark.stop();
                }
        }
}
