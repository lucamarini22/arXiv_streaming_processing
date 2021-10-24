### Execution

open a bash and start a ZooKeeper server:
```
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```
open a second bash and start the Kafka server:
```
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```
open a third bash and create topic arXiv:
```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic arXiv
```

create topic real\_time\_arXiv:
```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic real_time_arXiv
```

run the historical producer:
```
python3 historical_producer.py
```
open a fourth bash and run the historical consumer:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,org.mongodb.spark:mongo-spark-connector_2.11:2.4.3 ./historical_consumer.py
```

open a fifth bash and run the real-time producer:
```
python3 real_time_producer.py
```
open a sixth bash and run the real-time consumer:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,org.mongodb.spark:mongo-spark-connector_2.11:2.4.3 ./real_time_consumer.py
```

### See historical and real-time papers' info on MongoDB Charts
```
cd mongodb-charts
```

```
sudo docker-compose up -d
```

go to http://localhost:8080 and login with the credentials (email, password) that are present in docker-compose.yaml



### Package Requirements (Python 3.7)
feedparser: 5.2.1

kafka-python: 2.0.2

pyspark: 2.4.3

pymongo: 3.12.0              


### Platforms versions
Spark Streaming: 2.4.3

Kafka: 2.0.0

mongod: 5.0.3
