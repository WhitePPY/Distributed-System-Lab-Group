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
* Install Kafka-Python
    ```bash
    pip install kafka-python
    ```
* Start Kafka Server
    ```bash
    # run zookeeper
    ./kafka_2.13-3.6.0/bin/zookeeper-server-start.sh kafka_2.13-3.6.0/config/zookeeper.properties
    # run server
    ./kafka_2.13-3.6.0/bin/kafka-server-start.sh kafka_2.13-3.6.0/config/server.properties
    # create topic and modify partitions
    ./kafka_2.13-3.6.0/bin/kafka-topics.sh --create --topic testTopic --partitions [NUM_OF_NODES] --replication-factor 1 --bootstrap-server localhost:9092
    ```
## Run the Demo ##
* Run Distributed Nodes
    ```bash
    # for each nodes:
    spark-submit consumer.py
    ```
* Run Main Program
    ```bash
    python producer.py
    ```
