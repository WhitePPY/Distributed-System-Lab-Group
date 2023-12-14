#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1

export SPARK_HOME=/home/dsys2313/yuanhao/kafka/kafka_2.13-3.6.0

export PYSPARK_PYTHON=/var/scratch/dsys2313/miniconda3/bin/python

$SPARK_HOME/bin/zookeeper-server-start.sh $SPARK_HOME/config/zookeeper.properties &

echo 'zookeeper done'

$SPARK_HOME/bin/kafka-server-start.sh $SPARK_HOME/config/server.properties &

echo 'kafka done'

sleep 30

$SPARK_HOME/bin/kafka-topics.sh --create --topic Topic1 --partitions 2 --replication-factor 1 --bootstrap-server localhost:9092

echo 'topic 1 done'

$SPARK_HOME/bin/kafka-topics.sh --create --topic Topic2 --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

echo 'topic 2 done'

node1=`squeue | grep dsys2313 | awk -F ' ' '{print $8}'`

echo $node1

$PYSPARK_PYTHON distributor.py $node1

$PYSPARK_PYTHON integrator.py $node1
