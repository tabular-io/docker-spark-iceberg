#!/bin/bash

docker buildx build -t tabulario/spark-iceberg:latest -t tabulario/spark-iceberg:3.2.1_0.13.1 --platform=linux/amd64,linux/arm64 ./spark --push

