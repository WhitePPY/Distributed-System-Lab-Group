#!/bin/bash
#SBATCH --nodes=2
#SBATCH --time=00:15:00

# Start the PySpark application using srun

spark-submit processor.py $1 $4
