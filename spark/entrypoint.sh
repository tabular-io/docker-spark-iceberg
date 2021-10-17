#!/bin/bash

export SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)

schematool -dbType postgres -initSchema && hive --service metastore &

/opt/spark/sbin/start-master.sh -p 7077
/opt/spark/sbin/start-worker.sh spark://spark-iceberg:7077

spark-sql -f /opt/spark/bootstrap/ddl.sql
pyspark < /opt/spark/bootstrap/load_movielens_data.py

tail -f /dev/null