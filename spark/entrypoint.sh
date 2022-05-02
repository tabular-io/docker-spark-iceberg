#!/bin/bash

/opt/spark/sbin/start-master.sh -p 7077
/opt/spark/sbin/start-worker.sh spark://spark-iceberg:7077
/opt/spark/sbin/start-history-server.sh

pyspark-notebook
