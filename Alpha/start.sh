#!/bin/bash

sbatch server.sh

sleep 10

node1=`squeue | grep dsys2313 | awk -F ' ' '{print $8}'`

echo $node1

sleep 10

sbatch worker.sh $node1
