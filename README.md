# Spark + Iceberg Quickstart Image

This is a docker compose environment to quickly get up and running with a Spark environment and a local Iceberg
catalog. It uses a postgres database as a JDBC catalog.  

**note**: If you don't have docker installed, you can head over to the [Get Docker](https://docs.docker.com/get-docker/)
page for installation instructions.

# Usage
First, start up the `spark-iceberg` and `postgres` container by running:
```
docker-compose up
```

Next, run any of the following commands, depending on which shell you prefer to use:
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

To reset the catalog and data, remove the `postgres` and `warehouse` directories.

For more information on getting started with using Iceberg, checkout
the [Getting Started](https://iceberg.apache.org/getting-started/) guide in the official docs.
