package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random




object KafkaSpark {
  def main(args: Array[String]) {

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setMaster("local[2]").setAppName("Task1")
    val ssc = new StreamingContext(conf, Seconds(1))
    // checkpoint directory
    ssc.checkpoint("./checkpoint")
    // Kafka parameters
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )
    
    val topics: String = "arXiv"
    val topicsSet = topics.split(",").toSet

    // get messages from Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, 
      kafkaConf, 
      topicsSet
    )
    // visualize messages
    messages.print()
    //messages.count().print()
    
    /*val values = messages.map(m => m._2)
    val pairs = values.map(_.split(",")).map(v => (v(0), v(1).toDouble))

    // measure the average value for each key in a stateful manner
    // in the state it's also present the oldCounter of the key (it's the 2nd parameter -> Int), otherwise they would be lost when changing RDD 
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
      // get new value of key
      val newValue = value.getOrElse(0.0) 
      var newCounter = 0
      var currAvg = 0.0
      // get oldAvg and oldCounter of current key from state 
      val (oldAvg, oldCounter) = state.getOption.getOrElse(0.0, 0)
      // update occurences of current key
      newCounter = oldCounter + 1
      // compute  new average of current key
      val newAvg = ( (oldAvg * oldCounter) + newValue) / newCounter
      state.update( (newAvg, newCounter) )
      (key, newAvg)
    }
    
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
    // visualize final results
    stateDstream.print()
    */
    ssc.start()
    ssc.awaitTermination()
  }
}


