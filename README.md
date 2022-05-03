# Spark + Iceberg Quickstart Image

This is a docker compose environment to quickly get up and running with a Spark environment and a local Iceberg
catalog. It uses a postgres database as a JDBC catalog.  

**note**: If you don't have docker installed, you can head over to the [Get Docker](https://docs.docker.com/get-docker/)
page for installation instructions.

## Usage
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
docker exec -it spark-iceberg notebook
```

To stop the service, just run `docker-compose down`.

## Troubleshooting & Maintenance

### Resetting Catalog Data
To reset the catalog and data, remove the `postgres` and `warehouse` directories.
```bash
docker-compose down && docker-compose kill && rm -rf ./postgres && rm -rf ./warehouse
```

### Refreshing Docker Image
The prebuilt spark image is uploaded to Dockerhub. Out of convenience, the image tag defaults to `latest`.

If you have an older version of the image, you might need to remove it to upgrade.
```bash
docker image rm tabulario/spark-iceberg && docker-compose pull
```

For more information on getting started with using Iceberg, checkout
the [Getting Started](https://iceberg.apache.org/getting-started/) guide in the official docs.

The repository for the docker image is [located on dockerhub](https://hub.docker.com/r/tabulario/spark-iceberg).
