from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

import findspark
findspark.init() 

import json

import pymongo




# Create a MongoDB database and collection
#client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
#db_name = "arXiv_db"
#db = client[db_name]

#current_collection = "recent_papers"
#papers_collection = db[current_collection]

# Create a local StreamingContext with two working thread and batch interval of 3 second
sc = SparkContext("local", "arXivConsumer")
ssc = StreamingContext(sc, 3)
spark = SparkSession(sc) \
  .builder \
  .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
topic = "real_time_arXiv"

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
#df_paper_info = df_paper_info.withColumnRenamed('key', '_id')

#df_paper_info = df_paper_info.groupBy("main_category").count()
#df_paper_info = df_paper_info.withColumnRenamed('main_category', '_id')


'''
def write_mongo_row(df, epoch_id, db_name=db_name, collection_name=current_collection):
  mongoURL = "mongodb://127.0.0.1/{}.{}".format(db_name, collection_name)
  df.write.format("mongo").mode("append").option("uri", mongoURL).save()
  pass

query=df_paper_info \
  .writeStream \
  .outputMode("update") \
  .foreachBatch(write_mongo_row).start()
query.awaitTermination()
'''

df_num_papers_cat = df_paper_info.groupby(df_paper_info.main_category).count()

#val d = inputDataset.groupBy("realkey").agg(mean("realvalue"))

df_avg_pages_cat = df_paper_info \
  .filter(df_paper_info.page_num > 0) \
  .groupBy(df_paper_info.main_category) \
  .agg(func.mean('page_num'))

# need to write this, but I get an error
final_df = df_num_papers_cat.join(df_avg_pages_cat, 
  df_num_papers_cat.main_category == df_avg_pages_cat.main_category, 'inner')

# write dataframe to terminal to debug
ds = df_avg_pages_cat \
  .writeStream \
  .outputMode("complete") \
  .format("console") \
  .start() \
  .awaitTermination()

#.outputMode("complete") \
spark.stop()



