#!/bin/bash
#SBATCH --time=00:15:00
#SBATCH -N 1

zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

echo 'zookeeper done'

sleep 10

kafka-server-start.sh $KAFKA_HOME/config/server.properties &

echo 'kafka done'

sleep 20

kafka-topics.sh --create --topic Topic1 --partitions $3 --replication-factor 1 --bootstrap-server localhost:9092

echo 'topic 1 done'

kafka-topics.sh --create --topic Topic2 --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

echo 'topic 2 done'

node1=`squeue | grep dsys2313 | awk -F ' ' '{print $8}'`

echo $node1

sleep 5

python distributor.py $node1

python integrator.py $node1
