# README #
## Environment Configuration ##
* Install PySpark
    ```bash
    pip install pyspark
    ```
* Install Kafka
    ```bash
    wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
    tar -xzf kafka_2.13-3.6.0.tgz
    ```
* Start Kafka Server
    ```bash
    ./kafka_2.13-3.6.0/bin/zookeeper-server-start.sh config/zookeeper.properties
    ./kafka_2.13-3.6.0/bin/kafka-server-start.sh config/server.properties
    ./kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic testTopic --partitions 2
    ```
## Run the Demo ##
* Run Distributed Nodes
    ```bash
    spark-submit consumer.py
    ```
* Run Main Program
    ```bash
    python producer.py
    ```