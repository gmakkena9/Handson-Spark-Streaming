from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_df = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Print parsed data to the CSV files
def write_to_csv(batch_df, batch_id):
    batch_df.write.mode("append").option("header", True).csv("outputs/task_1")

query = parsed_df.writeStream \
    .foreachBatch(write_to_csv) \
    .outputMode("append") \
    .start()

query.awaitTermination()
