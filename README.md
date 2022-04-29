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

### Use `Dockerfile` In This Repo
To directly use the Dockerfile in this repo (as opposed to pulling the hosted `tabulario/spark-iceberg` image), change the following line in any of the docker-compose files.
```
-    image: tabulario/spark-iceberg
+    build: spark/
```

### Deploying Changes
To deploy changes to the hosted docker image `tabulario/spark-iceberg`, run the following. (Requires access to the tabulario docker hub account)
```
cd spark
docker buildx build -t tabulario/spark-iceberg --platform=linux/amd64,linux/arm64 . --push
```

---

For more information on getting started with using Iceberg, checkout
the [Getting Started](https://iceberg.apache.org/getting-started/) guide in the official docs.
