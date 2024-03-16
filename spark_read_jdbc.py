
driverName = "com.snowflake.client.jdbc.SnowflakeDriver";

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.set('spark.jars', './jars/snowflake-jdbc-3.13.30.jar,./jars/spark-snowflake_2.12-2.12.0-spark_3.2.jar')

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("snowflake-test") \
    .config(conf=conf) \
    .getOrCreate()

SNOWFLAKE_JDBC_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver"

preds = ["first_name = 'roshan'", "first_name  = 'ram'", "first_name  = 'test'", "first_name  = 'shrihari'"]

predicate_expr = " AND ".join(preds)

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:snowflake://fc82263.central-india.azure.snowflakecomputing.com") \
    .option("dbtable", "test_table") \
    .option("driver", SNOWFLAKE_JDBC_DRIVER) \
    .option("user", "") \
    .option("password", "") \
    .option("db", "testdb") \
    .option("schema", "testschema") \
    .option("partitionColumn", "ID") \
    .option("lowerBound", "1") \
    .option("upperBound", "4") \
    .option("numPartitions", "4") \
    .load()


#  \

jdbcDF.printSchema()
print(f'count: {jdbcDF.count()}')
print(f'partitions: {jdbcDF.rdd.getNumPartitions()}')
print(f"Default Parallelism: {int(spark.sparkContext.defaultParallelism)}")

print("maxPartitionBytes:", spark.conf.get("spark.sql.files.maxPartitionBytes"))
rdd_size_mb = int(spark.conf.get("spark.sql.files.maxPartitionBytes").rstrip("bB")) / (1024 * 1024)
print("maxPartitionMB:", rdd_size_mb, "MB")


sfOptions_write= {
    "sfURL": "https://fc82263.central-india.azure.snowflakecomputing.com",
    "sfUser" : "",
    "sfPassword" : "",
    "sfDatabase" : "testdb",
    "sfSchema" : "testschema",
    "sfRole" : "accountadmin",
    "sfWarehouse" : "COMPUTE_WH",
    "truncate_table": "on",
    "usestagingtable": "off",
    "partition_size_in_mb": "128"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
jdbcDF.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions_write).option("dbTable", "test_table_new").mode("append").save()