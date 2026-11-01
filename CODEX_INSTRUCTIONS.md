# Codex Instructions — Polygon Image Data Engineering Platform

## 1. Project Context

**GitHub Repository:** https://github.com/bhoomisingh00079/Datapipeline-polyMaster

**Project Name:** Polygon Image Data Engineering Platform

Build a complete, end-to-end data engineering platform using the supplied Polygon Image Dataset.

The dataset contains PNG polygon images and an associated `targets.csv` metadata file.

The objective is to build a practical computer-vision dataset ingestion, validation, transformation, processing, and cloud-storage pipeline.

This is also a learning-oriented portfolio project. Implement industry-relevant data engineering technologies while keeping the implementation understandable, modular, testable, and demonstrable.

Do not assume this is an image classification project. The primary focus is data engineering, not model training.

## 2. My Environment and Preferences

- Operating system: macOS
- Hardware: Apple Silicon Mac M1
- Primary language: Python
- Cloud platform: AWS
- Cloud storage: Amazon S3
- Implementation preference: Fast implementation with Codex doing most of the coding.
- Learning preference: Explain important implementation decisions, commands, architecture, and technical concepts so I can understand and demonstrate the project.

Use ARM64-compatible Docker images and dependencies wherever possible.

## 3. Required Technology Stack

Implement the following technologies in a meaningful way:

### Core Pipeline
- Python
- Pandas
- NumPy
- Pillow
- PyArrow
- Boto3

### Cloud
- AWS S3
- AWS IAM

### Distributed Processing
- Apache Spark
- PySpark

### Orchestration
- Apache Airflow

### Streaming
- Apache Kafka
- Spark Structured Streaming

### Data Lake
- Delta Lake
- Parquet

### Infrastructure
- Docker
- Docker Compose

### Testing
- Pytest

Do not add technologies merely for decoration. Every component must have a clear purpose and working integration.

## 4. CA1 Assignment Requirements — Highest Priority

Read the supplied CA1 assignment PDF carefully.

The baseline assignment requires:

1. Data extraction from the supplied source.
2. Data transformation using Pandas.
3. Data cleaning and validation.
4. Storage in AWS S3 or equivalent cloud object storage.
5. A documented storage structure.
6. Execution evidence.
7. A technical report of 3–5 pages.
8. A demonstration video.

The CA1 pipeline MUST remain independently executable without requiring Kafka, Spark, Airflow, or Delta Lake.

Implement the following execution modes:

**Mode A: ca1**

Python + Pandas + Pillow + PyArrow + Boto3 + AWS S3.

This is the mandatory assignment implementation.

**Mode B: batch-full**

Docker + PySpark + Airflow + AWS S3 + optional Delta Lake.

This is the advanced batch-processing implementation.

**Mode C: stream-full**

Kafka + Spark Structured Streaming + AWS S3 + Delta Lake.

This demonstrates event-based streaming using simulated image-ingestion events.

Do not make the CA1 mode dependent on the advanced infrastructure.

## 5. Dataset Inspection

Inspect the actual dataset ZIP before implementing extraction.

The ZIP contains:
- Polygon PNG images.
- `targets.csv` containing image metadata.

The metadata is expected to include fields such as:

- filename
- sides
- bg_color
- fg_color
- bound_circle_x
- bound_circle_y
- bound_circle_r
- rotation

There may also be an exported index column such as `Unnamed: 0`.

These are preliminary observations, not guaranteed schema assumptions. Verify the actual files.

Your implementation must:

1. Discover the ZIP's internal directory structure dynamically.
2. Ignore `__MACOSX` and irrelevant archive metadata.
3. Locate `targets.csv` dynamically.
4. Locate image files dynamically.
5. Count images and metadata rows.
6. Check duplicate filenames.
7. Check missing metadata values.
8. Check invalid image files.
9. Check image dimensions and color modes.
10. Verify CSV-to-image filename matching.
11. Identify invalid or inconsistent records.
12. Record a clear data-quality report.

Do not silently discard invalid records.

Do not assume all images have the same dimensions without verification.

Keep the original PNG images unchanged.

## 6. Architecture

### A. Batch Pipeline

Dataset ZIP
→ Python extraction
→ Image inventory using Pillow
→ Metadata extraction
→ Pandas validation and transformation
→ PySpark distributed processing
→ Parquet output
→ AWS S3
→ Delta Lake curated tables

### B. Streaming Pipeline

Image event simulator
→ Kafka producer
→ Kafka topic: polygon-image-events
→ Spark Structured Streaming consumer
→ S3 event archive
→ Delta Lake event table

The streaming pipeline must use simulated events generated from the static dataset.

Never describe the supplied ZIP as a live data source.

### C. Airflow Orchestration

Create an Airflow DAG that orchestrates:

1. Input validation
2. Dataset extraction
3. Image inventory
4. Metadata validation
5. Pandas transformation
6. PySpark batch processing
7. S3 upload
8. Output verification
9. Pipeline summary generation

Airflow is the orchestration layer, not the data-processing engine.

## 7. Data Transformation

Use Pandas for the mandatory CA1 transformation.

Include:

- Explicit data type conversion.
- Missing-value checks.
- Duplicate detection.
- Filename normalization where appropriate.
- Metadata and image reconciliation.
- Business-rule validation.
- Image-property enrichment.
- Structured processing logs.

Generate a clean metadata dataset and a data-quality report.

Do not invent imputation values. If no missing values exist, document that no imputation was required.

Verify whether `Unnamed: 0` is redundant before removing it.

## 8. Image Processing

Use Pillow to inspect and validate the images.

Generate an image manifest containing useful attributes such as:

- filename
- image width
- image height
- color mode
- image format
- file size
- validation status

Keep image files in PNG format.

Store structured metadata and analytical outputs in Parquet or Delta format. Do not convert image binaries into Parquet.

## 9. AWS S3 Integration

Use Boto3 for S3 integration.

Proposed bucket layout:

polygon-image-platform/
  raw/
    targets.csv
    images/
  validated/
    image_manifest/
  processed/
    ingestion_date=YYYY-MM-DD/
  streaming/
    events/
  delta/
    polygon_metadata/
    image_events/

Use configuration through environment variables.

Never hardcode AWS access keys, secret keys, or bucket credentials.

Create an `.env.example` file.

Do not make the bucket public.

Use least-privilege IAM permissions.

Do not create paid AWS resources or perform potentially costly operations without asking me first.

The project should support local testing without AWS credentials using a clearly separated local storage option.

## 10. Apache Spark

Use PySpark for the advanced batch-processing layer.

Implement:

- Reading validated metadata.
- Schema enforcement.
- Distributed transformations.
- Aggregations and analytics.
- Parquet writing.
- S3 integration.

Demonstrate meaningful Spark operations even though the supplied dataset may not require distributed processing for performance.

Document why Spark is useful for larger datasets.

## 11. Apache Kafka

Create a simulated event producer.

Topic:

`polygon-image-events`

Example event fields:

- event_id
- filename
- event_type
- event_timestamp
- validation_status

Generate a small demo stream of approximately 20–100 events.

Implement a Kafka consumer through Spark Structured Streaming.

Include appropriate error handling and checkpointing.

Clearly distinguish simulated streaming from the original batch dataset.

## 12. Delta Lake

Implement Delta Lake as an advanced curated storage layer.

Demonstrate:

- Delta table creation.
- Reading and writing.
- Schema enforcement.
- Schema evolution where appropriate.
- Update or merge operations if meaningful.
- Version history or time travel demonstration.

Retain Parquet outputs for the mandatory CA1 requirement.

## 13. Docker and Docker Compose

Create a Docker Compose configuration for the advanced infrastructure.

Organize services logically:

- Kafka
- Spark
- Airflow
- Supporting services where required

Use Docker Compose profiles so that users can start only the infrastructure needed.

Avoid starting every service by default.

Use ARM64-compatible images and pinned versions.

Limit container resource usage to suit an M1 Mac.

Do not remove existing Docker volumes or containers without asking me.

## 14. Testing

Use Pytest.

Include tests for:

- ZIP extraction.
- CSV loading.
- Schema validation.
- Missing-value detection.
- Duplicate detection.
- Image integrity.
- Filename reconciliation.
- Pandas transformation.
- Parquet output.
- S3 integration where credentials are available.
- Spark transformations.
- Streaming event processing.

Include sample fixtures so unit tests can run without the full dataset or AWS credentials.

## 15. Repository Structure

Use a clean, modular structure similar to:

polygon-image-data-platform/
│
├── dags/
├── src/
│   ├── common/
│   ├── ingestion/
│   ├── validation/
│   ├── transformation/
│   ├── storage/
│   ├── streaming/
│   └── pipeline.py
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│
├── docker/
├── scripts/
├── docs/
│
├── data/
│   ├── raw/
│   ├── staging/
│   └── processed/
│
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── CODEX_INSTRUCTIONS.md
└── plan.md

Adjust the structure if the existing repository already has a useful organization.

Do not overwrite existing work without inspecting it.

The dataset and generated large files must be excluded from Git.

## 16. Implementation Strategy

Implement the project in phases.

**Phase 0 — Repository and Environment Inspection**
- Inspect the existing GitHub repository and working tree.
- Read all supplied project documents.
- Inspect the dataset.
- Check Python, Java, Docker, and available dependencies.
- Prepare a technology compatibility matrix.
- Identify configuration requirements.

**Phase 1 — Dataset Ingestion**
- Implement ZIP extraction.
- Discover files dynamically.
- Build the image inventory.

**Phase 2 — Data Validation**
- Validate metadata and images.
- Generate a data-quality report.

**Phase 3 — CA1 Pandas Pipeline**
- Implement extraction and transformation.
- Generate clean output and Parquet files.
- Make this independently runnable.

**Phase 4 — AWS S3**
- Implement Boto3 upload.
- Configure credentials securely.
- Verify uploaded objects.

**Phase 5 — Docker**
- Containerize the infrastructure.
- Create Compose profiles.

**Phase 6 — PySpark**
- Implement distributed batch processing.

**Phase 7 — Airflow**
- Implement and test the orchestration DAG.

**Phase 8 — Kafka**
- Implement event simulation and Kafka producer.

**Phase 9 — Spark Structured Streaming**
- Consume and process Kafka events.

**Phase 10 — Delta Lake**
- Implement curated Delta tables.

**Phase 11 — Testing and Documentation**
- Run tests.
- Document architecture and commands.
- Prepare CA1 report and demo evidence.

Do not implement everything blindly in one step. Complete and verify each phase before proceeding.

## 17. Documentation and Final Deliverables

Maintain:

- README.md
- Architecture diagram
- Data dictionary
- Compatibility matrix
- Setup guide
- Execution guide
- AWS configuration guide
- Troubleshooting guide
- Testing documentation
- CA1 evidence checklist

The final project must include:

1. Working CA1 pipeline.
2. Working AWS S3 integration.
3. Working Spark batch pipeline.
4. Working Airflow DAG.
5. Working Kafka producer and consumer.
6. Working Spark Structured Streaming demonstration.
7. Delta Lake integration.
8. Docker Compose infrastructure.
9. Automated tests.
10. Technical report and demo instructions.

Explain every major component in the README so that I can present the architecture and implementation confidently.

## 18. Important Rules

- Never fabricate execution results.
- Never claim a component works before testing it.
- Never silently skip errors.
- Never hardcode credentials.
- Never commit the dataset or secrets.
- Do not change the original images.
- Do not make the CA1 pipeline dependent on advanced technologies.
- Prefer maintainable, modular code over unnecessary complexity.
- Use pinned, compatible dependency versions.
- Explain important commands and technical decisions.
- Ask before paid cloud resources or destructive operations.

## 19. First Task — Start Here

Do not start implementing all phases immediately.

First inspect the existing repository and all supplied files.

Read:
- CODEX_INSTRUCTIONS.md
- plan.md
- docs/CA1_Assignment.pdf
- the supplied dataset ZIP

Then report:

1. Current repository structure.
2. Existing files and implementation, if any.
3. Actual dataset structure and statistics.
4. CA1 requirements and how they map to the proposed pipeline.
5. Mac M1 environment compatibility.
6. Required installations and dependencies.
7. Proposed implementation order.
8. Any blockers or decisions requiring my input.

After presenting the findings, begin Phase 0 and prepare the environment/setup files.

Wait for verification before moving to the next phase.