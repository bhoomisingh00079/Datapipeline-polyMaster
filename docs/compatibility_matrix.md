# Compatibility matrix and execution status

The versions below are the selected implementation matrix. The CA1 path has been run locally; advanced services still need container execution and end-to-end verification. Pin container image digests before a production deployment.

| Area | Candidate | Role and compatibility notes | Phase |
|---|---|---|---|
| Host Python | CPython 3.10.11, arm64 | Available on this Mac; supported by PySpark 4.x and Airflow 3.x. Isolate with a project virtual environment. | CA1 |
| Pandas / NumPy / Pillow / PyArrow / Boto3 | pandas 2.3.3; numpy 2.2.6; Pillow 12.2.0; pyarrow 23.0.1; boto3 1.42.81 | Candidate pins matching versions detected on the host. Reconfirm wheel availability for Python 3.10/arm64 when installing. | CA1 |
| Pytest | 8.4.2 | CA1 test dependency; missing from the current system Python. | CA1 |
| Spark / PySpark | 4.0.5 | Optional distributed batch execution; Python 3.10+ and Java 17 or 21. Apache's image tag is published for `linux/arm64`. | Batch |
| Java | Temurin/OpenJDK 21, arm64 | The host currently has Java 24, which is outside the chosen Spark 4.0 line's supported Java 17/21 range. Use Java 21 for the Spark services. | Batch/stream |
| Delta Lake | delta-spark 4.0.1 with Spark 4.0.x | The Delta release matrix lists Delta 4.0.1 as compatible with Spark 4.0.x. | Advanced |
| Airflow | Apache Airflow 3.3.2, Python 3.10 image, arm64 | Separate Compose profile; not installed in the CA1 virtual environment. Use the official versioned image. | Orchestration |
| Kafka | Apache Kafka 4.2.2 KRaft image, arm64; kafka-python 3.0.12 | The official image tag is published for `linux/arm64`; use the pure-Python client for the producer. | Streaming |
| Docker | Docker Engine + Compose | Docker CLI 29.7.2 and Compose 5.4.0 are present; the daemon is currently inaccessible from this session. ARM64 image support remains to be checked at service startup. | Infrastructure |
| AWS | Boto3 + private S3 bucket + least-privilege IAM | Local storage is the default. No AWS resources or uploads are part of Phase 0. Credentials must come from the AWS profile/standard credential chain, never `.env.example`. | CA1/advanced |

## Execution boundaries

- `ca1` uses Python, Pandas, Pillow, PyArrow, Boto3, and local storage by default. It does not require Docker, Java, Spark, Airflow, or Kafka.
- Spark, Airflow, Kafka, and Delta Lake are optional extensions with separate dependencies/services.
- The Kafka stream will simulate events from this static ZIP; it will not be represented as a live source.

## Host observations (2026-10-31)

- macOS on Apple Silicon (`arm64`).
- System Python: 3.10.11. A separate existing project at `~/Desktop/PolyMetric` has a Python 3.14.8 environment; this repository will use its own environment.
- System Java: 24.0.2. Use Java 21 for the candidate Spark setup.
- Docker CLI: 29.7.2; Compose: 5.4.0. Docker socket access failed with permission denied, so container execution has not been verified.
- Core packages found in system Python: pandas 2.3.3, numpy 2.2.6, Pillow 12.2.0, pyarrow 23.0.1, boto3 1.42.81. PySpark and pytest are absent.

## Official compatibility references

- [Spark platform overview](https://spark.apache.org/docs/latest/): lists supported Java/Python versions and platform support.
- [PySpark installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html): Python and JVM requirements.
- [Delta Lake release compatibility](https://docs.delta.io/releases/): Delta/Spark version matrix.
- [Airflow prerequisites](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html): Python support and local runtime constraints.
- [Official Airflow image tags](https://hub.docker.com/r/apache/airflow/tags): available image tags and architectures.
- [Official Kafka Docker image tags](https://hub.docker.com/r/apache/kafka/tags): available image tags and architectures.
- [kafka-python on PyPI](https://pypi.org/project/kafka-python/): pure-Python client release and supported Python versions.
