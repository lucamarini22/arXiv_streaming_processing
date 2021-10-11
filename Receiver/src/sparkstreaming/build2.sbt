name := "spark_kafka"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.3",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.3",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.4.3"
)

