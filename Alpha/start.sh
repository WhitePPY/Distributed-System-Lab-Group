#!/bin/bash

sbatch server.sh

sleep 30

node1=`squeue | grep dsys2313 | awk -F ' ' '{print $8}'`
top_k=$1
echo $node1

sleep 1

sbatch worker.sh $node1 $top_k
