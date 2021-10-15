FROM ubuntu:16.04

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get -y install \
    sudo \
    curl \
    vim \
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

RUN mkdir /opt/hadoop && mkdir /opt/spark

WORKDIR /opt/spark

RUN curl https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -o hadoop-3.3.1.tar.gz
RUN curl https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz -o spark-3.1.2-bin-hadoop3.2.tgz
RUN curl -L https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.12.0/iceberg-spark3-runtime-0.12.0.jar -o iceberg-spark3-runtime-0.12.0.jar

RUN tar xvzf hadoop-3.3.1.tar.gz --directory /opt/hadoop --strip-components 1
RUN rm -rf hadoop-3.3.1.tar.gz

RUN tar xvzf spark-3.1.2-bin-hadoop3.2.tgz --directory /opt/spark --strip-components 1
RUN rm -rf spark-3.1.2-bin-hadoop3.2.tgz

RUN cp iceberg-spark3-runtime-0.12.0.jar /opt/spark/jars

RUN mkdir -p /home/iceberg/warehouse /home/iceberg/notebooks

# Add a pyspark-notebook command for convenience
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook
RUN echo 'PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/pyspark-notebook
RUN echo "PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook
RUN echo "pyspark" >> /bin/pyspark-notebook
RUN chmod u+x /bin/pyspark-notebook

COPY spark-defaults.conf /opt/spark/conf
COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
