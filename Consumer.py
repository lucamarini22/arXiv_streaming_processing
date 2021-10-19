from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

import findspark
findspark.init() 

import json

import pymongo


def write_mongo_row(df, epoch_id):
  mongoURL = "mongodb://127.0.0.1/arXiv_db.papers"
  df.write.format("mongo").mode("append").option("uri",mongoURL).save()
  pass

# Create a MongoDB database and collection
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = client["arXiv_db"]
papers_collection = db["papers"]

# Create a local StreamingContext with two working thread and batch interval of 3 second
sc = SparkContext("local", "arXivConsumer")
ssc = StreamingContext(sc, 3)
spark = SparkSession(sc) \
  .builder \
  .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/arXiv_db.papers") \
  .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/arXiv_db.papers") \
  .getOrCreate()


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
        StructField('isVersionOne', BooleanType(), True),
        StructField('published_year', IntegerType(), True),
        StructField('published_month', IntegerType(), True),
        StructField('published_day', IntegerType(), True),
        StructField('first_author', StringType(), True),
        StructField('page_num', IntegerType(), True),
        StructField('main_category', StringType(), True),
        StructField('categories', ArrayType(StringType()), True),
        StructField('human_readable_main_category', StringType(), True),
        StructField('human_readable_categories', ArrayType(StringType()), True)
    ]
)

# dataframe that contains papers info
df_paper_info = df.withColumn("value", from_json("value", schema)) \
    .select(col('value.*')) 
# rename column key to _id to use it as primary key of each document in MongoDB
df_paper_info = df_paper_info.withColumnRenamed('key', '_id')


query=df_paper_info.writeStream.foreachBatch(write_mongo_row).start()
query.awaitTermination()

'''
# write dataframe to terminal to debug
ds = df_paper_info \
  .writeStream \
  .format("console") \
  .start() \
  .awaitTermination()
'''

spark.stop()

