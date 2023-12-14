#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:02:00

# Load the necessary modules

# Set the Spark home directory
export SPARK_HOME=/home/dsys2313/yuanhao/spark_test/spark-3.5.0-bin-hadoop3

# Set the Python executable for PySpark
export PYSPARK_PYTHON=/var/scratch/dsys2313/miniconda3/bin/python

# Start the PySpark application using srun
$SPARK_HOME/bin/spark-submit  processor.py $1
