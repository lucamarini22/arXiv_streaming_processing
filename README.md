### Execution

open a bash and start a ZooKeeper server:
```
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```
open a second bash and start the Kafka server:
```
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```
open a third bash and create topi arXiv:
```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic arXiv
```
run the Producer:
```
python3 Producer.py
```
open a fourth bash and run the consumer:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 ./Consumer2.py
```

### Package Requirements (Python 3.7)
feedparser: 5.2.1

kafka-python: 2.0.2

pyspark: 2.4.3


### Platforms versions
Spark Streaming: 2.4.3

Kafka: 2.0.0


