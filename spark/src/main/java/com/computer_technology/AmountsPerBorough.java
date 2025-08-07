package com.computer_technology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class AmountsPerBorough {
    public static void main(String[] args) {
        try (SparkSession spark = Session.GetSpark()) {
            Dataset<Row> yellowTrips = Session.yellowTrips(spark);
            Dataset<Row> taxiZones = Session.taxiZones(spark);

            Dataset<Row> tripsWithDestinationBorough = yellowTrips.join(
                    taxiZones,
                    yellowTrips.col("DOLocationID").equalTo(taxiZones.col("LocationID")),
                    "inner"
            );

            Dataset<Row> boroughSummary = tripsWithDestinationBorough
                    .groupBy(col("Borough"))
                    .agg(
                            sum("total_amount").alias("total_revenue"),
                            count(lit(1)).alias("trip_count")
                    )
                    .orderBy(col("total_revenue").desc());

            boroughSummary.show();

            boroughSummary
                    .select(
                            col("Borough"),
                            col("total_revenue").cast("decimal(15,2)").alias("total_revenue"),
                            col("trip_count")
                    )
                    .write()
                    .mode("overwrite")
                    .csv("hdfs://namenode1:9000/output/q3_revenue_by_borough.parquet");

            spark.stop();
        }
    }
}
