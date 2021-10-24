from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession
import pandas
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, TimestampType
import time
import findspark
findspark.init() 
import uuid
import pymongo

db_name = "arXiv_db"
#db = client[db_name]

current_collection = "real_papers"

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

df.printSchema()
# cast value col from binary to String
df = df.withColumn("value", df["value"].cast("string"))
#df1.printSchema()

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

uuidUdf= func.udf(lambda : str(uuid.uuid4()),StringType())

# dataframe that contains category paper count within 10s window


df_num_papers_cat = df_paper_info \
  .withColumn('time', func.current_timestamp()) \
  .withWatermark("time", "5 seconds") \
  .groupby(func.window("time", "10 seconds"), col("human_readable_main_category")) \
  .count()

df_num_papers_cat.printSchema()

# dataframe that contains category page average count within 10s window
df_avg_pages_cat = df_paper_info \
  .withColumn('time', func.current_timestamp()) \
  .withWatermark("time", "15 seconds") \
  .filter(col("page_num") > 0) \
  .groupBy(func.window("time", "10 seconds"), col("human_readable_main_category")) \
  .agg(func.mean(col('page_num'))).alias("time")


# write both dataframe to terminal to debug

global_cnt_cnt = 0
global_avg_cnt = 0

def foreach_batch_cnt(df, epoch_id):
    global global_cnt_cnt
    df.persist()
    pdf = df.toPandas()
    if not pdf.empty:
        global_cnt_cnt += 1
        pdf.to_csv('cnt_csv' + str(global_cnt_cnt) + '.csv')
    df.unpersist()

def foreach_batch_avg(df, epoch_id):
    global global_avg_cnt
    df.persist()
    pdf = df.toPandas()
    if not pdf.empty:
        global_avg_cnt += 1
        pdf.to_csv('avg_csv' + str(global_avg_cnt) + '.csv')
    df.unpersist()

def write_mongo_row(df, epoch_id, db_name=db_name, collection_name=current_collection):
  mongoURL = "mongodb://127.0.0.1/{}.{}".format(db_name, collection_name)
  df.write.format("mongo").mode("append").option("uri", mongoURL).save()
  #df.write.format("console").mode("append").save()
  

dss = df_num_papers_cat \
  .writeStream.foreachBatch(foreach_batch_cnt).start()

dss = df_num_papers_cat \
  .writeStream.foreachBatch(write_mongo_row).start().awaitTermination()

'''
dss2 = df_avg_pages_cat \
  .writeStream.foreachBatch(foreach_batch_avg).start().awaitTermination()
'''
spark.stop()



