## Spark


```python
## TODO: Set spark up with required configurations!
```

## Load Data


```python
yellow_tripdata_df = spark.read.parquet('/home/hananeh/yellow_trip_data_parquet')
taxi_zone_lookup_df = spark.read.csv('/home/hananeh/taxi_zone_lookup.csv', header=True)
```

                                                                                    


```python
yellow_tripdata_df.printSchema()
```

    root
     |-- VendorID: integer (nullable = true)
     |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
     |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
     |-- passenger_count: long (nullable = true)
     |-- trip_distance: double (nullable = true)
     |-- RatecodeID: long (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- PULocationID: integer (nullable = true)
     |-- DOLocationID: integer (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- total_amount: double (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- Airport_fee: double (nullable = true)
    



```python
taxi_zone_lookup_df.printSchema()
```

    root
     |-- LocationID: string (nullable = true)
     |-- Borough: string (nullable = true)
     |-- Zone: string (nullable = true)
     |-- service_zone: string (nullable = true)
    


## Analysis

### Analysis 1


```python
analysis_1_result = (
    yellow_tripdata_df
    .filter((F.col('passenger_count') > 2) & (F.col('trip_distance') > 5))
    .withColumn('duration_minutes', 
                ((F.unix_timestamp(F.col('tpep_dropoff_datetime')) - 
                  (F.unix_timestamp(F.col('tpep_pickup_datetime')))) / 60))
    .orderBy(F.desc('duration_minutes'))    
)

analysis_1_result.show(5)
```

    [Stage 2:========================================>                 (7 + 3) / 10]

    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
    |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|  duration_minutes|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
    |       2| 2024-09-26 16:40:25|  2024-09-27 16:39:30|              5|         9.98|         1|                 N|         138|         189|           1|       54.1|  7.5|    0.5|     12.97|         0.0|                  1.0|       77.82|                 0.0|       1.75|1439.0833333333333|
    |       2| 2024-09-10 16:07:51|  2024-09-11 16:06:46|              5|        18.94|         2|                 N|         132|          93|           1|       70.0|  5.0|    0.5|     17.54|        6.94|                  1.0|      105.23|                 2.5|       1.75|1438.9166666666667|
    |       2| 2024-09-21 20:38:49|  2024-09-22 20:37:38|              5|         5.49|         1|                 N|         114|         140|           1|       33.8|  1.0|    0.5|       0.0|         0.0|                  1.0|        38.8|                 2.5|        0.0|1438.8166666666666|
    |       2| 2024-09-20 14:57:32|  2024-09-21 14:55:28|              6|         7.94|         1|                 N|         170|         181|           1|       42.9|  0.0|    0.5|      9.38|         0.0|                  1.0|       56.28|                 2.5|        0.0|1437.9333333333334|
    |       2| 2024-09-09 15:59:21|  2024-09-10 15:57:14|              5|        19.28|         1|                 N|         132|         133|           1|       80.0|  2.5|    0.5|     17.15|         0.0|                  1.0|       102.9|                 0.0|       1.75|1437.8833333333334|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
    only showing top 5 rows
    


                                                                                    


```python
analysis_1_result.write.parquet('/home/hananeh/test_files/q1_long_trips_df.parquet')
```

                                                                                    

### Analysis 2


```python
analysis_2_result = (
    yellow_tripdata_df
    .join(
        taxi_zone_lookup_df,
        on=(yellow_tripdata_df.PULocationID == taxi_zone_lookup_df.LocationID),
        how='left'
    )
    .groupBy('Zone', 'Borough') # This is fine since each Zone has only one Borough
    .agg(
        F.mean('fare_amount').alias('mean_fare_amount')
    )
    .select(
        'Zone',
        'Borough',
        'mean_fare_amount',
    )
)

analysis_2_result.show()
```

    +--------------------+---------+------------------+
    |                Zone|  Borough|  mean_fare_amount|
    +--------------------+---------+------------------+
    |        East Village|Manhattan|15.638300306124805|
    |          Whitestone|   Queens|37.414482758620686|
    |Long Island City/...|   Queens| 21.91831953239162|
    |   Battery Park City|Manhattan| 23.20857142857146|
    |                SoHo|Manhattan| 17.02121989653328|
    |         Old Astoria|   Queens|  23.5892962962963|
    |       South Jamaica|   Queens| 46.78904255319148|
    |          Mount Hope|    Bronx| 29.53035928143713|
    |             Bayside|   Queens| 36.06983050847458|
    |        West Village|Manhattan|15.426540600410743|
    |Upper East Side S...|Manhattan|13.470830136177682|
    |        Saint Albans|   Queens|38.497886710239655|
    |     Manhattan Beach| Brooklyn| 34.14525179856115|
    | Ocean Parkway South| Brooklyn|28.033185840707972|
    |          Ozone Park|   Queens| 35.62231111111112|
    |          Douglaston|   Queens| 28.83829268292683|
    |             Madison| Brooklyn|30.301799999999997|
    |Schuylerville/Edg...|    Bronx| 36.15587378640775|
    |Riverdale/North R...|    Bronx|  36.3378947368421|
    |      Middle Village|   Queens| 29.74390845070422|
    +--------------------+---------+------------------+
    only showing top 20 rows
    



```python
analysis_2_result.write.parquet('/home/hananeh/test_files/q2_avg_fare_by_zone.parquet')
```

### Analysis 3


```python
analysis_3_result = (
    yellow_tripdata_df
    .join(
        taxi_zone_lookup_df,
        on=(yellow_tripdata_df.DOLocationID == taxi_zone_lookup_df.LocationID),
        how='left'
    )
    .groupBy('Borough')
    .agg(
        F.sum('total_amount').alias('sum_total_amount'),
        F.count('*').alias('trip_count')
    )
    .select(
        'Borough',
        'sum_total_amount',
        'trip_count'
    )
)

analysis_3_result.show()
```

    +-------------+--------------------+----------+
    |      Borough|    sum_total_amount|trip_count|
    +-------------+--------------------+----------+
    |       Queens|1.0154144669998907E7|    193970|
    |          EWR|  1205927.7000000032|      9614|
    |      Unknown|  367945.76000000094|     13081|
    |     Brooklyn|   7577216.649999933|    161182|
    |Staten Island|   97063.12000000001|       967|
    |          N/A|  2040392.8199999984|     17481|
    |    Manhattan| 8.111937221006177E7|   3213990|
    |        Bronx|   1121335.930000009|     22745|
    +-------------+--------------------+----------+
    



```python
analysis_3_result.write.parquet('/home/hananeh/test_files/q3_revenue_by_borough.parquet')
```

### Analysis 4


```python
analysis_4_result = (
    yellow_tripdata_df
    .withColumn('date', F.to_date(F.col('tpep_dropoff_datetime')))
    .groupBy('date')
    .agg(
        F.max('tip_amount').alias('max_tip_amount')
    )
    .select(
        'date',
        'max_tip_amount',
    )
)

analysis_4_result.show()
```

    +----------+--------------+
    |      date|max_tip_amount|
    +----------+--------------+
    |2009-01-01|         17.54|
    |2024-09-10|         384.4|
    |2024-09-03|         200.0|
    |2024-09-12|         140.0|
    |2024-09-01|         100.0|
    |2024-09-08|         100.0|
    |2024-09-02|         99.99|
    |2024-09-07|         100.0|
    |2024-09-06|         150.0|
    |2024-09-09|          99.0|
    |2024-08-31|           7.9|
    |2024-09-05|        101.24|
    |2024-09-11|         500.0|
    |2024-09-16|         145.0|
    |2024-09-04|         125.0|
    |2024-09-18|         99.99|
    |2024-09-22|         410.0|
    |2024-09-21|        112.34|
    |2024-09-19|         200.0|
    |2024-09-20|         175.0|
    +----------+--------------+
    only showing top 20 rows
    



```python
analysis_4_result.write.parquet('/home/hananeh/test_files/q4_max_tip_per_day.parquet')
```
