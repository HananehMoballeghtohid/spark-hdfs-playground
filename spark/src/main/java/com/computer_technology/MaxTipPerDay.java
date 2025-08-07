package com.computer_technology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class MaxTipPerDay {
    public static void main(String[] args) {
        try (SparkSession spark = Session.GetSpark()) {
            Dataset<Row> trips = Session.yellowTrips(spark)
                    .withColumn("trip_date", to_date(col("tpep_pickup_datetime")));

            Dataset<Row> maxTipPerDay = trips
                    .groupBy(col("trip_date"))
                    .agg(max(col("tip_amount")).alias("max_tip"));

            Dataset<Row> tripsWithMaxTip = trips
                    .join(maxTipPerDay,
                            trips.col("trip_date").equalTo(maxTipPerDay.col("trip_date"))
                                    .and(trips.col("tip_amount").equalTo(maxTipPerDay.col("max_tip"))),
                            "inner"
                    )
                    .select(trips.col("*"))
                    .orderBy(trips.col("trip_date"));

            tripsWithMaxTip.show();

            maxTipPerDay.write()
                    .mode("overwrite")
                    .parquet("hdfs://namenode1:9000/output/q4_max_tip_per_day.parquet");

            spark.stop();
        }
    }
}
