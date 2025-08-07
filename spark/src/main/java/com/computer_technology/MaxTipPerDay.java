package com.computer_technology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class MaxTipPerDay {
    public static void main(String[] args) {
        try (SparkSession spark = Session.GetSpark()) {
            Dataset<Row> maxTipPerDay = Session.yellowTrips(spark)
                    .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
                    .groupBy(col("trip_date"))
                    .agg(max(col("tip_amount")).alias("max_tip"))
                    .orderBy(col("trip_date"));

            maxTipPerDay.show();
            maxTipPerDay.write()
                    .mode("overwrite")
                    .parquet("hdfs://namenode1:9000/output/q4_max_tip_per_day.parquet");

            spark.stop();
        }
    }
}
