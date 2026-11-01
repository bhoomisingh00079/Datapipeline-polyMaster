FROM apache/spark:4.0.5-scala2.13-java21-python3-ubuntu

USER root
COPY requirements-ca1.txt requirements-spark.txt requirements-stream.txt /tmp/polygon-requirements/
RUN python3 -m pip install --no-cache-dir \
    -r /tmp/polygon-requirements/requirements-ca1.txt \
    -r /tmp/polygon-requirements/requirements-spark.txt \
    -r /tmp/polygon-requirements/requirements-stream.txt
WORKDIR /opt/project
ENV PYSPARK_PYTHON=python3
