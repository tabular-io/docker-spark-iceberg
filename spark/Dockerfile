FROM ubuntu:16.04

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

# Remove python 3.5 that comes with ubuntu 16.04 and symlink python 3.9
RUN apt remove python3.5-minimal -y
RUN ln -sf /usr/bin/python3.9 /usr/bin/python3

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
        && python3.9 get-pip.py \
        && rm get-pip.py
RUN pip3 install jupyter pandas

RUN mkdir /opt/hadoop && mkdir /opt/spark && mkdir /opt/hive

WORKDIR /opt/spark

RUN curl https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -o hadoop-3.3.1.tar.gz
RUN curl https://dlcdn.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz -o apache-hive-3.1.2-bin.tar.gz
RUN curl https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz -o spark-3.1.2-bin-hadoop3.2.tgz
RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.24.jar -o postgresql-42.2.24.jar
RUN curl -L https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.12.0/iceberg-spark3-runtime-0.12.0.jar -o iceberg-spark3-runtime-0.12.0.jar

RUN tar xvzf hadoop-3.3.1.tar.gz --directory /opt/hadoop --strip-components 1
RUN rm -rf hadoop-3.3.1.tar.gz

RUN tar xvzf apache-hive-3.1.2-bin.tar.gz --directory /opt/hive --strip-components 1
RUN rm -rf apache-hive-3.1.2-bin.tar.gz

RUN tar xvzf spark-3.1.2-bin-hadoop3.2.tgz --directory /opt/spark --strip-components 1
RUN rm -rf spark-3.1.2-bin-hadoop3.2.tgz

RUN cp postgresql-42.2.24.jar /opt/hive/lib
RUN cp postgresql-42.2.24.jar /opt/spark/jars
RUN cp postgresql-42.2.24.jar /opt/hadoop/share/hadoop/common
RUN cp iceberg-spark3-runtime-0.12.0.jar /opt/spark/jars

RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse

# Add a pyspark-notebook command for convenience
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook
RUN echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/pyspark-notebook
RUN echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook
RUN echo "pyspark" >> /bin/pyspark-notebook
RUN chmod u+x /bin/pyspark-notebook

COPY bootstrap /opt/spark/bootstrap
RUN curl -L https://files.grouplens.org/datasets/movielens/ml-25m.zip -o ml25m.zip
RUN unzip ml25m.zip -d /opt/spark/bootstrap/data

COPY spark-defaults.conf /opt/spark/conf
COPY hive-site.xml /opt/spark/conf
COPY hive-site.xml /opt/hive/conf

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
