#Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Create a Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("writetohive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Define Function to Write to Hive
def write_to_hive(batch_df, batch_id):
    print(f"Processing batch: {batch_id}")
    hive_df = batch_df.select("numbers", "contract_name", "banking", "bike_stands",
                               "available_bike_stands", "available_bikes", "address",
                               "status", "position", "timestamps")

    print("Sample data in the batch:")
    hive_df.show()
    hive_df.write.saveAsTable(name="bikes_stations.bikes_stations", format="hive", mode='append')
    print("Data written to Hive successfully.")

#Create Hive Database and Use It
spark.sql("CREATE DATABASE IF NOT EXISTS bikes_stations")
spark.sql("USE bikes_stations")

#Define Hive Table Schema
hive_table_schema = """
    CREATE TABLE IF NOT EXISTS bikes_stations (
        numbers INT,
        contract_name STRING,
        banking STRING,
        bike_stands INT,
        available_bike_stands INT,
        available_bikes INT,
        address STRING,
        status STRING,
        position STRUCT<lat: DOUBLE, lng: DOUBLE>,
        timestamps STRING
    )
"""

#Create Hive Table
spark.sql(hive_table_schema)

#Define Schema for Kafka Data
schema = StructType([
    StructField("numbers", IntegerType(), True),
    StructField("contract_name", StringType(), True),
    StructField("banking", StringType(), True),
    StructField("bike_stands", IntegerType(), True),
    StructField("available_bike_stands", IntegerType(), True),
    StructField("available_bikes", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("position", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True)
    ]), True),
    StructField("timestamps", StringType(), True),
])

#Read Streaming Data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bike") \
    .option("startingOffsets", "latest") \
    .load()

#Extract JSON Data and Modify Structure
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")
json_df = json_df.withColumn("position", col("position").alias("position").cast("struct<lat:double, lng:double>"))

#Filter Data
zero_bikes_df = json_df.filter(col("available_bikes") == 0)

#Write Stream to Hive
hive_query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(write_to_hive) \
    .option("checkpointLocation", "/home/mariem/bike_stations_hive/checkpoints/new") \
    .start()
hive_query.awaitTermination()

