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
RUN apt remove python3.5-minimal -y \
 && ln -sf /usr/bin/python3.9 /usr/bin/python3

# Install Jupyter and other python deps
FROM iceberg-spark-demo-env-builder AS iceberg-spark-demo-jupyter-installer

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
 && python3 get-pip.py \
 && rm get-pip.py \
 && pip3 install jupyter pandas


## TODO - Might want to move this layer above the jupyter one as this is the most expensive and we want it highest so that subsequent changes to later parts of the file won't uncache this expensive stage
## Download spark and hadoop dependencies and install
FROM iceberg-spark-demo-jupyter-installer AS iceberg-spark-demo-spark-deps-downloader

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Prelude - Initial setup and inexpensive things to cache (mostly commands that actually dont invoke an additional docker image layer preferrably)
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# TODO - Possibly make prefix configurable so users can change to Apache archives (or change to archives ourselves).
RUN curl https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -o hadoop-3.3.1.tar.gz
RUN tar xvzf hadoop-3.3.1.tar.gz --directory /opt/hadoop --strip-components 1 \
 && rm -rf hadoop-3.3.1.tar.gz

RUN curl https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz -o spark-3.1.2-bin-hadoop3.2.tgz
RUN tar xvzf spark-3.1.2-bin-hadoop3.2.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.1.2-bin-hadoop3.2.tgz

RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.24.jar -o postgresql-42.2.24.jar \
 && cp postgresql-42.2.24.jar /opt/spark/jars \
 && cp postgresql-42.2.24.jar /opt/hadoop/share/hadoop/common \
 && rm postgresql-42.2.24.jar

# TODO - Move this to prelude and give logical prefixes as env variables (like LOCAL_WAREHOUSE) etc. This command is an unnecessary image layer.
RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse

# Add a pyspark-notebook command for convenience
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/pyspark-notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook \
 && echo "pyspark" >> /bin/pyspark-notebook \
 && chmod u+x /bin/pyspark-notebook

COPY spark-defaults.conf /opt/spark/conf

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
