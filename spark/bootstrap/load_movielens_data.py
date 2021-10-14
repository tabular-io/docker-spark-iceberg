import os

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('load_movielens_data') \
    .enableHiveSupport() \
    .getOrCreate()

DATA_ROOT_DIR = "/opt/spark/bootstrap/data/ml-25m"

spark.read.format("csv").option("header", "true").load(os.path.join(DATA_ROOT_DIR, "movies.csv")).registerTempTable("movies")
spark.read.format("csv").option("header", "true").load(os.path.join(DATA_ROOT_DIR, "ratings.csv")).registerTempTable("ratings")
spark.read.format("csv").option("header", "true").load(os.path.join(DATA_ROOT_DIR, "tags.csv")).registerTempTable("tags")
spark.read.format("csv").option("header", "true").load(os.path.join(DATA_ROOT_DIR, "links.csv")).registerTempTable("links")
spark.read.format("csv").option("header", "true").load(os.path.join(DATA_ROOT_DIR, "genome-scores.csv")).registerTempTable("genome_scores")
spark.read.format("csv").option("header", "true").load(os.path.join(DATA_ROOT_DIR, "genome-tags.csv")).registerTempTable("genome_tags")

for dataset in ["movies", "ratings", "tags", "links"]:
  spark.sql(f"insert into local.movies.{dataset} select * from {dataset}")

for dataset in ["genome-scores", "genome-tags"]:
  spark.sql(f"insert into local.{dataset.replace('-','.')} select * from {dataset.replace('-', '_')}")
