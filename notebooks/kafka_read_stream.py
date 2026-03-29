from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

from dotenv import load_dotenv
import os

load_dotenv()

#from databricks.connect import DatabricksSession
#spark = DatabricksSession.builder.getOrCreate()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()


schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("division", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("coupon_applied", BooleanType(), True),
    StructField("event_timestamp", StringType(), True),
])

raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "172.18.171.11:9092")
        .option("subscribe", "retail-pos-stream")
        .option("startingOffsets", "earliest")
        .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

query = (
    parsed_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("checkpointLocation", "C:/tmp/spark-checkpoint")
        .start()
)

query.awaitTermination()