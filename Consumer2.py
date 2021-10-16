from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

import findspark
findspark.init() 

import json 

# Create a local StreamingContext with two working thread and batch interval of 3 second
sc = SparkContext("local", "arXivConsumer")
ssc = StreamingContext(sc, 3)
spark = SparkSession(sc)

topic = "arXiv"

# Spark reads from Kafka 
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092,localhost:2181") \
  .option("subscribe", topic) \
  .load()

# cast value col from binary to String
df = df.withColumn("value", df["value"].cast("string"))
df.printSchema()

# schema of value column received from Producer
schema = StructType(
    [
        StructField('key', StringType(), True),
        StructField('title', StringType(), True),
        StructField('first_author', StringType(), True),
        StructField('categories', StringType(), True)
    ]
)

# dataframe that contains papers info
df_paper_info = df.withColumn("value", from_json("value", schema)) \
    .select(col('key'), col('topic'), col('value.*'), 
        col('partition'), col('offset'), col('timestamp'), col('timestampType'))

# write dataframe to terminal
ds = df_paper_info \
  .writeStream \
  .format("console") \
  .start() \
  .awaitTermination()





spark.stop()

