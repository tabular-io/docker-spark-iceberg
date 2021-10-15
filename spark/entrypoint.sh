#!/bin/bash

export SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)

/opt/spark/sbin/start-master.sh -p 7077
/opt/spark/sbin/start-worker.sh spark://spark-iceberg:7077

alias pyspark-notebook="PYSPARK_DRIVER_PYTHON=jupyter-notebook PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --port=8888 --no-browser --allow-root\" pyspark"

tail -f /dev/null