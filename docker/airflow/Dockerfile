ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && JAVA_HOME_DIR="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && ln -s "${JAVA_HOME_DIR}" /usr/lib/jvm/default-java \
    && java -version
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements-ca1.txt requirements-spark.txt requirements-stream.txt /tmp/polygon-requirements/
RUN grep -vE '^[[:space:]]*(pyarrow|boto3)==' /tmp/polygon-requirements/requirements-ca1.txt \
      > /tmp/polygon-requirements/requirements-ca1-airflow.txt \
    && echo 'boto3' >> /tmp/polygon-requirements/requirements-ca1-airflow.txt \
    && chmod 644 /tmp/polygon-requirements/requirements-ca1-airflow.txt

USER airflow
RUN python -m pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt" \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/polygon-requirements/requirements-ca1-airflow.txt \
      -r /tmp/polygon-requirements/requirements-spark.txt \
      -r /tmp/polygon-requirements/requirements-stream.txt \
    && python -m pip install --no-cache-dir --no-deps pyarrow==23.0.1
ENV PYSPARK_PYTHON=python
ENV PYTHONPATH=/opt/airflow
