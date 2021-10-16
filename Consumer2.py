from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession

import findspark
findspark.init() 

import json 

# Create a local StreamingContext with two working thread and batch interval of 3 second
sc = SparkContext("local", "arXivConsumer")
ssc = StreamingContext(sc, 3)
spark = SparkSession(sc)


topic = "arXiv"



df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092,localhost:2181") \
  .option("subscribe", topic) \
  .load()

df.printSchema()

#kafkaStream = KafkaUtils.createDirectStream(ssc, ['arXiv'], kafkaParams)

#kafkaStream.print()

#parsed = kafkaStream.map(lambda v: json.loads(v))


#fore = parsed.foreachRDD(f) 
'''
val printQuery2 = d.writeStream
                       .outputMode(org.apache.spark.sql.streaming.OutputMode.Update())
                       .format("console")
                       .start()
    printQuery2.awaitTermination()
  
    spark.stop()
'''

ds = df \
  .writeStream \
  .format("console") \
  .start() \
  .awaitTermination()
  
  #.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  #.option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  #.option("topic", "topic1") \

spark.stop()
#ssc.start() # Start the computation
#ssc.awaitTermination() # Wait for the computation to terminate 
