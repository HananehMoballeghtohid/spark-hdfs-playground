package com.computer-technology-sharif;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class SparkHadoopHAApp {
    private static final Logger logger = LoggerFactory.getLogger(SparkHadoopHAApp.class);
    private static final String HDFS_CLUSTER_NAME = "hadoop-cluster";
    
    public static void main(String[] args) {
        logger.info("Starting Spark Hadoop HA Application...");

        SparkSession spark = null;
        try {
            // Create Spark Session with HA configuration
            spark = createSparkSession();
            
            // Test ZooKeeper connectivity
            testZooKeeperConnection();
            
            // Test HDFS HA connectivity
            testHDFSHA(spark);
            
            logger.info("Application completed successfully!");
            
        } catch (Exception e) {
            logger.error("Application failed: ", e);
            System.exit(1);
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    private static SparkSession createSparkSession() {
        logger.info("Creating Spark Session with HA configuration...");
        
        SparkConf conf = new SparkConf()
                .setAppName("Java Spark Hadoop HA Application")
                .set("spark.master", "spark://spark-master:7077")
                .set("spark.executor.memory", "2g")
                .set("spark.executor.cores", "2")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "hdfs://hadoop-cluster/spark-logs")
                .set("spark.history.fs.logDirectory", "hdfs://hadoop-cluster/spark-logs");

        return SparkSession.builder()
                .config(conf)
                .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-cluster")
                .config("spark.hadoop.dfs.nameservices", "hadoop-cluster")
                .config("spark.hadoop.dfs.ha.namenodes.hadoop-cluster", "namenode1,namenode2")
                .config("spark.hadoop.dfs.namenode.rpc-address.hadoop-cluster.namenode1", "namenode1:9000")
                .config("spark.hadoop.dfs.namenode.rpc-address.hadoop-cluster.namenode2", "namenode2:9000")
                .config("spark.hadoop.dfs.namenode.http-address.hadoop-cluster.namenode1", "namenode1:9870")
                .config("spark.hadoop.dfs.namenode.http-address.hadoop-cluster.namenode2", "namenode2:9870")
                .config("spark.hadoop.dfs.client.failover.proxy.provider.hadoop-cluster", 
                       "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
                .config("spark.hadoop.ha.zookeeper.quorum", "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181")
                .getOrCreate();
    }

    private static void testZooKeeperConnection() throws Exception {
        logger.info("Testing ZooKeeper connectivity...");
        
        String zkConnectString = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181";
        CountDownLatch connectedSignal = new CountDownLatch(1);
        
        ZooKeeper zk = new ZooKeeper(zkConnectString, 3000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        
        connectedSignal.await();
        logger.info("ZooKeeper connection established. Session ID: {}", zk.getSessionId());
        
        // List ZooKeeper nodes
        List<String> children = zk.getChildren("/", false);
        logger.info("ZooKeeper root children: {}", children);
        
        zk.close();
    }

    private static void testHDFSHA(SparkSession spark) throws IOException {
        logger.info("Testing HDFS HA connectivity...");
        
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        FileSystem fs = FileSystem.get(conf);
        
        logger.info("HDFS FileSystem: {}", fs.getClass().getName());
        logger.info("Default FS: {}", fs.getUri());
        
        // Test basic HDFS operations
        Path testDir = new Path("/tmp/spark-ha-test");
        
        // Create test directory
        if (fs.exists(testDir)) {
            fs.delete(testDir, true);
        }
        fs.mkdirs(testDir);
        logger.info("Created test directory: {}", testDir);
        
        // List root directory
        FileStatus[] status = fs.listStatus(new Path("/"));
        logger.info("HDFS Root directory contents:");
        for (FileStatus fileStatus : status) {
            logger.info("  {} - {} bytes", fileStatus.getPath(), fileStatus.getLen());
        }
        
        // Test file operations
        Path testFile = new Path(testDir, "test-file.txt");
        try (var out = fs.create(testFile)) {
            out.writeBytes("Hello from Spark Hadoop HA Test!
");
            out.writeBytes("Timestamp: " + new Date() + "
");
        }
        
        // Read file back
        try (var in = fs.open(testFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            logger.info("Test file content: {}", content.trim());
        }
        
        fs.close();
    }

    private static void runSparkJobs(SparkSession spark) {
        logger.info("Running Spark jobs...");
        
        // Job 1: Basic RDD operations with HDFS
        runRDDJob(spark);
        
        // Job 2: DataFrame operations with HDFS persistence
        runDataFrameJob(spark);
        
        // Job 3: SQL operations with temporary views
        runSQLJob(spark);
        
        // Job 4: Streaming-like batch processing
        runBatchProcessingJob(spark);
    }

    private static void runRDDJob(SparkSession spark) {
        logger.info("Running RDD job...");
        
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        // Generate large dataset
        List<Integer> numbers = new ArrayList<>();
        for (int i = 1; i <= 100000; i++) {
            numbers.add(i);
        }
        
        JavaRDD<Integer> numbersRDD = jsc.parallelize(numbers, 10);
        
        // Perform complex transformations
        JavaRDD<Integer> processedRDD = numbersRDD
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .filter(n -> n > 100);
        
        // Save to HDFS
        String outputPath = "hdfs://hadoop-cluster/tmp/spark-output/rdd-job-" + System.currentTimeMillis();
        processedRDD.saveAsTextFile(outputPath);
        
        // Calculate statistics
        long count = processedRDD.count();
        int sum = processedRDD.reduce((a, b) -> a + b);
        double average = (double) sum / count;
        
        logger.info("RDD Job Results:");
        logger.info("  Count: {}", count);
        logger.info("  Sum: {}", sum);
        logger.info("  Average: {}", average);
        logger.info("  Output saved to: {}", outputPath);
    }

    private static void runDataFrameJob(SparkSession spark) {
        logger.info("Running DataFrame job...");
        
        // Create sample employee data
        List<Employee> employees = Arrays.asList(
            new Employee(1, "John Doe", "Engineering", 75000, "2020-01-15"),
            new Employee(2, "Jane Smith", "Marketing", 65000, "2019-03-20"),
            new Employee(3, "Bob Johnson", "Engineering", 80000, "2018-07-10"),
            new Employee(4, "Alice Brown", "HR", 60000, "2021-05-12"),
            new Employee(5, "Charlie Davis", "Engineering", 85000, "2017-09-05"),
            new Employee(6, "Eva Wilson", "Marketing", 70000, "2020-11-08"),
            new Employee(7, "Frank Miller", "Finance", 72000, "2019-12-01"),
            new Employee(8, "Grace Lee", "Engineering", 78000, "2021-02-28")
        );
        
        // Create DataFrame
        Dataset<Row> employeeDF = spark.createDataFrame(employees, Employee.class);
        
        // Register as temporary view
        employeeDF.createOrReplaceTempView("employees");
        
        // Perform DataFrame operations
        logger.info("Employee DataFrame Schema:");
        employeeDF.printSchema();
        
        logger.info("Sample data:");
        employeeDF.show(5);
        
        // Department-wise statistics
        Dataset<Row> deptStats = employeeDF
                .groupBy("department")
                .agg(
                    org.apache.spark.sql.functions.count("*").alias("employee_count"),
                    org.apache.spark.sql.functions.avg("salary").alias("avg_salary"),
                    org.apache.spark.sql.functions.max("salary").alias("max_salary"),
                    org.apache.spark.sql.functions.min("salary").alias("min_salary")
                );
        
        logger.info("Department-wise statistics:");
        deptStats.show();
        
        // Save to HDFS in Parquet format
        String outputPath = "hdfs://hadoop-cluster/tmp/spark-output/dataframe-job-" + System.currentTimeMillis();
        deptStats.write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);
        
        logger.info("DataFrame results saved to: {}", outputPath);
        
        // Save in JSON format as well
        String jsonPath = outputPath + "-json";
        employeeDF.write()
                .mode(SaveMode.Overwrite)
                .json(jsonPath);
        
        logger.info("Employee data saved as JSON to: {}", jsonPath);
    }

    private static void runSQLJob(SparkSession spark) {
        logger.info("Running SQL job...");
        
        // Create sample sales data
        List<Sale> sales = new ArrayList<>();
        Random random = new Random();
        String[] products = {"Laptop", "Phone", "Tablet", "Monitor", "Keyboard"};
        String[] regions = {"North", "South", "East", "West"};
        
        for (int i = 1; i <= 10000; i++) {
            sales.add(new Sale(
                i,
                products[random.nextInt(products.length)],
                regions[random.nextInt(regions.length)],
                100 + random.nextInt(1900), // Price between 100-2000
                1 + random.nextInt(5),      // Quantity 1-5
                "2023-" + String.format("%02d", 1 + random.nextInt(12)) + "-" + 
                String.format("%02d", 1 + random.nextInt(28))
            ));
        }
        
        Dataset<Row> salesDF = spark.createDataFrame(sales, Sale.class);
        salesDF.createOrReplaceTempView("sales");
        
        // Complex SQL queries
        String query1 = """
            SELECT 
                region,
                product,
                SUM(price * quantity) as total_revenue,
                AVG(price) as avg_price,
                COUNT(*) as transaction_count
            FROM sales 
            GROUP BY region, product
            ORDER BY total_revenue DESC
            """;
        
        Dataset<Row> result1 = spark.sql(query1);
        logger.info("Revenue by Region and Product:");
        result1.show(20);
        
        String query2 = """
            WITH monthly_sales AS (
                SELECT 
                    SUBSTRING(sale_date, 1, 7) as month,
                    SUM(price * quantity) as monthly_revenue
                FROM sales
                GROUP BY SUBSTRING(sale_date, 1, 7)
            )
            SELECT 
                month,
                monthly_revenue,
                LAG(monthly_revenue) OVER (ORDER BY month) as prev_month_revenue,
                monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month) as revenue_change
            FROM monthly_sales
            ORDER BY month
            """;
        
        Dataset<Row> result2 = spark.sql(query2);
        logger.info("Monthly Revenue Trends:");
        result2.show();
        
        // Save complex query results to HDFS
        String outputPath = "hdfs://hadoop-cluster/tmp/spark-output/sql-job-" + System.currentTimeMillis();
        result1.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("region")
                .parquet(outputPath);
        
        logger.info("SQL job results saved to: {}", outputPath);
    }

    private static void runBatchProcessingJob(SparkSession spark) {
        logger.info("Running batch processing job...");
        
        try {
            // Create sample log data
            List<LogEntry> logs = generateLogData();
            Dataset<Row> logsDF = spark.createDataFrame(logs, LogEntry.class);
            
            // Process logs in batches
            String basePath = "hdfs://hadoop-cluster/tmp/spark-output/batch-processing-" + System.currentTimeMillis();
            
            // Partition by log level and timestamp
            logsDF.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("level", "date")
                    .parquet(basePath + "/raw");
            
            // Aggregate error logs
            Dataset<Row> errorSummary = logsDF
                    .filter("level = 'ERROR'")
                    .groupBy("date", "service")
                    .agg(
                        org.apache.spark.sql.functions.count("*").alias("error_count"),
                        org.apache.spark.sql.functions.collect_list("message").alias("error_messages")
                    );
            
            errorSummary.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(basePath + "/error-summary");
            
            logger.info("Batch processing completed. Results saved to: {}", basePath);
            
            // Show sample results
            logger.info("Error Summary:");
            errorSummary.show(10, false);
            
        } catch (Exception e) {
            logger.error("Batch processing job failed: ", e);
        }
    }

    private static List<LogEntry> generateLogData() {
        List<LogEntry> logs = new ArrayList<>();
        Random random = new Random();
        String[] levels = {"INFO", "WARN", "ERROR", "DEBUG"};
        String[] services = {"web-server", "database", "cache", "auth-service", "api-gateway"};
        String[] messages = {
            "Request processed successfully",
            "Database connection timeout",
            "Cache miss for key",
            "Authentication failed",
            "Memory usage high",
            "Disk space low",
            "Network latency detected"
        };
        
        for (int i = 1; i <= 50000; i++) {
            logs.add(new LogEntry(
                i,
                new Date(System.currentTimeMillis() - random.nextInt(30) * 24 * 60 * 60 * 1000L),
                levels[random.nextInt(levels.length)],
                services[random.nextInt(services.length)],
                messages[random.nextInt(messages.length)]
            ));
        }
        
        return logs;
    }

    private static void demonstrateFailover(SparkSession spark) {
        logger.info("Demonstrating HA failover capabilities...");
        
        // This would typically involve more complex scenarios
        // For demonstration, we'll show how the application handles
        // multiple HDFS operations that could span failover events
        
        try {
            for (int i = 0; i < 5; i++) {
                String testPath = "hdfs://hadoop-cluster/tmp/failover-test-" + i + "-" + System.currentTimeMillis();
                
                // Create test data
                List<String> data = Arrays.asList(
                    "Failover test iteration: " + i,
                    "Timestamp: " + new Date(),
                    "Random data: " + UUID.randomUUID()
                );
                
                JavaRDD<String> rdd = spark.sparkContext().parallelize(data, 1).toJavaRDD();
                rdd.saveAsTextFile(testPath);
                
                // Verify data was written
                JavaRDD<String> readData = spark.sparkContext().textFile(testPath).toJavaRDD();
                long count = readData.count();
                
                logger.info("Failover test iteration {}: wrote and read {} lines to/from {}", i, count, testPath);
                
                // Small delay between operations
                Thread.sleep(2000);
            }
            
            logger.info("Failover demonstration completed successfully");
            
        } catch (Exception e) {
            logger.error("Failover demonstration failed: ", e);
        }
    }

    // Helper classes for data processing
    public static class Employee {
        private int id;
        private String name;
        private String department;
        private int salary;
        private String hire_date;

        public Employee() {}

        public Employee(int id, String name, String department, int salary, String hire_date) {
            this.id = id;
            this.name = name;
            this.department = department;
            this.salary = salary;
            this.hire_date = hire_date;
        }

        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }
        
        public int getSalary() { return salary; }
        public void setSalary(int salary) { this.salary = salary; }
        
        public String getHire_date() { return hire_date; }
        public void setHire_date(String hire_date) { this.hire_date = hire_date; }
    }

    public static class Sale {
        private int id;
        private String product;
        private String region;
        private int price;
        private int quantity;
        private String sale_date;

        public Sale() {}

        public Sale(int id, String product, String region, int price, int quantity, String sale_date) {
            this.id = id;
            this.product = product;
            this.region = region;
            this.price = price;
            this.quantity = quantity;
            this.sale_date = sale_date;
        }

        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        
        public String getProduct() { return product; }
        public void setProduct(String product) { this.product = product; }
        
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        
        public int getPrice() { return price; }
        public void setPrice(int price) { this.price = price; }
        
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        
        public String getSale_date() { return sale_date; }
        public void setSale_date(String sale_date) { this.sale_date = sale_date; }
    }

    public static class LogEntry {
        private int id;
        private Date timestamp;
        private String level;
        private String service;
        private String message;
        private String date;

        public LogEntry() {}

        public LogEntry(int id, Date timestamp, String level, String service, String message) {
            this.id = id;
            this.timestamp = timestamp;
            this.level = level;
            this.service = service;
            this.message = message;
            this.date = new java.text.SimpleDateFormat("yyyy-MM-dd").format(timestamp);
        }

        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        
        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { 
            this.timestamp = timestamp; 
            if (timestamp != null) {
                this.date = new java.text.SimpleDateFormat("yyyy-MM-dd").format(timestamp);
            }
        }
        
        public String getLevel() { return level; }
        public void setLevel(String level) { this.level = level; }
        
        public String getService() { return service; }
        public void setService(String service) { this.service = service; }
        
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        
        public String getDate() { return date; }
        public void setDate(String date) { this.date = date; }
    }
}

