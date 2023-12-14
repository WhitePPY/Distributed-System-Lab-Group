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
    # add Kafka to PATH
    vim ~/.bashrc
    export KAFKA_HOME=[YOUR_PATH]/kafka_2.13-3.6.0
    export PATH="$KAFKA_HOME/bin:$PATH"
    source ~/.bashrc
    # run zookeeper
    zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
    # run server
    kafka-server-start.sh $KAFKA_HOME/config/server.properties
    # create topic and modify partitions
    kafka-topics.sh --create --topic Topic1 --partitions [NUM_OF_NODES] --replication-factor 1 --bootstrap-server localhost:9092
    kafka-topics.sh --create --topic Topic2 --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
    ```
## Run the Demo ##
* Run Distributed Nodes
    ```bash
    # for each nodes:
    spark-submit processor.py
    ```
* Run Intergrator
    ```bash
    python integrator.py
    ```
* Run Main Program
    ```bash
    python distributor.py
    ```
