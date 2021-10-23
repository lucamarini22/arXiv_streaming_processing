from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

import findspark
findspark.init() 

import json

import pymongo

# Create a MongoDB database and collection
db_name = "arXiv_db"

current_collection = "papers"
#papers_collection = db[current_collection]

sc = SparkContext("local", "arXivConsumer")
spark = SparkSession(sc) \
  .builder \
  .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

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
        StructField('human_readable_categories', ArrayType(StringType()), True),
        StructField('abstract', StringType(), True),
        StructField('keywords', ArrayType(StringType()), True)
    ]
)

# dataframe that contains papers info
df_paper_info = df.withColumn("value", from_json("value", schema)) \
    .select(col('value.*'))
# rename column key to _id to use it as primary key of each document in MongoDB
df_paper_info = df_paper_info.withColumnRenamed('key', '_id')

def write_mongo_row(df, epoch_id, db_name=db_name, collection_name=current_collection):
  mongoURL = "mongodb://127.0.0.1/{}.{}".format(db_name, collection_name)
  df.write.format("mongo").mode("append").option("uri", mongoURL).save()
  pass

query=df_paper_info \
  .writeStream \
  .foreachBatch(write_mongo_row).start()
query.awaitTermination()

'''
# write dataframe to terminal to debug
ds = df_paper_info \
  .writeStream \
  .format("console") \
  .start() \
  .awaitTermination()
#  .trigger(processingTime='2 seconds') \
'''
spark.stop()


