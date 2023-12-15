#!/bin/bash

if [ $# -lt 5 ]; then
    echo "parameter1: user name"
    echo "parameter2: nodes amount"
    echo "parameter3: top K"
    echo "parameter4: file 1 name"
    echo "parameter5: file 2 name"
    exit 1
fi

sbatch server.sh $1 $2 $3 $4 $5

sleep 30

node1=`squeue | grep $1 | awk -F ' ' '{print $8}'`

echo $node1

sleep 1

sbatch worker.sh $node1 $1 $2 $3 $4 $5
