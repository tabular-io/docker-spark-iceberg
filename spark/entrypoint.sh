#!/bin/bash

start-master.sh -p 7077
start-worker.sh spark://spark-iceberg:7077
start-history-server.sh
start-thriftserver.sh

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi
