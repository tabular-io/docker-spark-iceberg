#!/bin/bash

set +ex

# wait for postgres
sleep 5

$HIVE_HOME/bin/schematool -dbType postgres -initSchema
$HIVE_HOME/hcatalog/sbin/hcat_server.sh start &

sleep 10

$HIVE_HOME/bin/hiveserver2 --hiveconf hive.root.logger=Info,console