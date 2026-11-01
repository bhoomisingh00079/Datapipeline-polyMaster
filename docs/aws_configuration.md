# AWS S3 configuration

S3 support is optional. Local CA1 execution is the default and does not contact AWS.
The uploader only writes to a bucket that already exists; it never creates a bucket,
changes bucket access settings, sets public ACLs, or stores credentials.

## Before an upload

1. Create or select a private S3 bucket yourself after reviewing AWS pricing for your
   region and expected storage/request volume. No bucket is created by this project.
2. Keep S3 Block Public Access enabled and configure default bucket encryption.
3. Provide a least-privilege role or profile with `s3:PutObject` and `s3:GetObject`
   permissions limited to the project's object prefix. `GetObject` is used by the
   uploader's `HeadObject` verification. The code does not require bucket creation,
   ACL changes, or `ListBucket`.
4. Configure an AWS CLI profile on the Mac. The Docker Compose services mount
   `~/.aws` read-only, so Boto3 can use that profile without copying credentials
   into the repository. Never put access keys in source code or `.env.example`.

## AWS Academy Learner Lab credentials

For AWS Academy Learner Lab, use the temporary credentials supplied by the lab;
do not run `aws configure sso` or create IAM access keys. Click **Start Lab**, wait
for the AWS status indicator to turn green, open **AWS Details**, then reveal or
download the **AWS CLI** credentials. Copy all three values—access key ID, secret
access key, and session token—into a named profile in `~/.aws/credentials` on the
Mac. The session token is required. These credentials expire and must be refreshed
after a new lab session. AWS describes this temporary credential workflow in its
[Learner Lab setup article](https://builder.aws.com/content/32E1EQtdsojEIqMZ4haf7CYLBuE/teaching-ai-agent-to-work-inside-aws-academy-learner-lab).

The project Compose profile mounts `~/.aws` read-only. Keep the credentials out
of the repository and `.env`.

Example environment (fill in only after configuring an existing bucket):

```bash
export AWS_PROFILE=polygon-pipeline
export AWS_REGION=ap-south-1
export S3_BUCKET=your-existing-private-bucket
export S3_PREFIX=polygon-image-platform
```

Also set the same values in the project's local `.env` for Compose. The presence
of AWS settings alone does not upload data: local storage remains the default.
For the Airflow end-to-end DAG, explicitly set its run configuration to
`storage_backend: s3` and provide `s3_bucket`. This uploads the raw images and
CA1 outputs and enables the streaming Parquet sink. Leave the DAG at its default
`local` setting to avoid cloud requests.

Then, only when you intend to upload the full run (including all source PNGs):

```bash
python scripts/upload_to_s3.py --run-dir data/staging/<run-id>
```

The uploader sends raw `targets.csv` and PNG objects, validation reports, and six
Parquet outputs. It verifies each object with `HeadObject` and checks the remote
content length against the local file size. A run over this supplied archive uploads
approximately 10,000 images plus metadata, reports, and analytical outputs; AWS
storage and request charges may apply. Local runs never invoke this command.

## Object layout

```text
s3://<bucket>/<prefix>/
  raw/ingestion_date=YYYY-MM-DD/run_id=<run-id>/targets.csv
  raw/ingestion_date=YYYY-MM-DD/run_id=<run-id>/images/<filename>.png
  validated/run_id=<run-id>/image_manifest.csv
  validated/run_id=<run-id>/quality_report.json
  validated/run_id=<run-id>/validation_issues.csv
  validated/run_id=<run-id>/*_summary.json
  processed/ingestion_date=YYYY-MM-DD/run_id=<run-id>/*.parquet
```

The input ZIP is not uploaded. Original PNG bytes are stored unchanged.
