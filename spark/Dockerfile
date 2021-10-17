# syntax=docker/dockerfile:1
FROM ubuntu:16.04 AS iceberg-spark-demo-env-builder

ENV PYTHON_VERSION=${PYTHON_VERSION:-"3.9"}

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get -y install \
    sudo \
    curl \
    vim \
    unzip \
    openjdk-8-jdk \
    python3.9 \
    python3.9-distutils \
    ssh

# Remove python 3.5 that comes with ubuntu 16.04 and symlink downloaded python
RUN apt remove python3.5-minimal -y
RUN ln -sf /usr/bin/python3.9 /usr/bin/python3

# Install Jupyter and other python deps
FROM iceberg-spark-demo-env-builder AS iceberg-spark-demo-jupyter-installer

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
        && python3 get-pip.py \
        && rm get-pip.py
RUN pip3 install jupyter pandas


## Download spark and hadoop dependencies and install
FROM iceberg-spark-demo-jupyter-installer AS iceberg-spark-demo-spark-deps-downloader

# TODO - Might want to have ability to not include this or to make it easier to switch if exec'ing in (which people will likely do). E.g. don't pre-install it but symlink as needed maybe.
# Docker build args
ARG iceberg="0.12.0"

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HIVE_HOME=${HIVE_HOME:-"/opt/hive"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Prelude - Initial setup and inexpensive things to cache (mostly commands that actually dont invoke an additional docker image layer preferrably)
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}  && mkdir -p  ${HIVE_HOME}
WORKDIR ${SPARK_HOME}

# Expensive things to do, so move them uo so as not to bust the docker image cache on rebuild.
# Download sample dataset
RUN mkdir -p /opt/spark/bootstrap
RUN curl -L https://files.grouplens.org/datasets/movielens/ml-25m.zip -o ml25m.zip
RUN unzip ml25m.zip -d /opt/spark/bootstrap/data

# TODO - Possibly make prefix configurable so users can change to Apache archives (or change to archives ourselves).
RUN curl https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -o hadoop-3.3.1.tar.gz
RUN tar xvzf hadoop-3.3.1.tar.gz --directory /opt/hadoop --strip-components 1 \
 && rm -rf hadoop-3.3.1.tar.gz

RUN curl https://dlcdn.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz -o apache-hive-3.1.2-bin.tar.gz
RUN tar xvzf apache-hive-3.1.2-bin.tar.gz --directory /opt/hive --strip-components 1 \
 && rm -rf apache-hive-3.1.2-bin.tar.gz

RUN curl https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz -o spark-3.1.2-bin-hadoop3.2.tgz
RUN tar xvzf spark-3.1.2-bin-hadoop3.2.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.1.2-bin-hadoop3.2.tgz

RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.24.jar -o postgresql-42.2.24.jar \
 && cp postgresql-42.2.24.jar /opt/hive/lib \
 && cp postgresql-42.2.24.jar /opt/spark/jars \
 && cp postgresql-42.2.24.jar /opt/hadoop/share/hadoop/common \
 && rm postgresql-42.2.24.jar

RUN curl -L https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/${iceberg}/iceberg-spark3-runtime-${iceberg}.jar -o iceberg-spark3-runtime-${iceberg}.jar \
 && cp iceberg-spark3-runtime-${iceberg}.jar /opt/spark/jars

# TODO - Move this to prelude and give logical prefixes as env variables (like LOCAL_WAREHOUSE) etc. This command is an unnecessary image layer.
RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse

# Add a pyspark-notebook command for convenience
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/pyspark-notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook \
 && echo "pyspark" >> /bin/pyspark-notebook \
 && chmod u+x /bin/pyspark-notebook

COPY spark-defaults.conf /opt/spark/conf
COPY hive-site.xml /opt/spark/conf
COPY hive-site.xml /opt/hive/conf

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
