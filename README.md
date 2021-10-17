# Spark + Iceberg Quickstart Image

This is a docker compose environment to quickly get up and running with a Spark environment and a local Iceberg
catalog. Currently, there is only a single service called `spark-iceberg`.  

**note**: If you don't have docker installed, you can head over to the [Get Docker](https://docs.docker.com/get-docker/)
page for installation instructions.

# Usage
First, start up the `metastore` and `spark-iceberg` services.
```
docker-compose up -d metastore
```
```
docker-compose up -d spark-iceberg
```

Next, run any of the following commands, depending on which shell you prefer to use.
```
docker exec -it spark-iceberg spark-shell
```
```
docker exec -it spark-iceberg spark-sql
```
```
docker exec -it spark-iceberg pyspark
```
```
docker exec -it spark-iceberg pyspark-notebook
```

To stop the service, just run `docker-compose down`.

For more information on getting started with using Iceberg, checkout
the [Getting Started](https://iceberg.apache.org/getting-started/) guide in the official docs.
