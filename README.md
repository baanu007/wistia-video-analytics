# Wistia Video Analytics Pipeline

End-to-end data pipeline on AWS that pulls video analytics from the Wistia
Stats API, lands them in S3, transforms to a star schema with PySpark on Glue,
and loads into Redshift Serverless. Orchestrated by AWS Glue Workflows.

## Architecture

```
Wistia API (4 endpoints)
     |
     v
Glue Python Shell job: wistia-ingest
     |
     v
S3 Bronze (raw JSON, partitioned by date)
     |
     v
Glue PySpark job: wistia-transform
     |
     v
S3 Silver (clean Parquet)
     |
     v
Glue Python Shell job: wistia-dq-and-load
     |
     v (DQ checks -> Redshift COPY via redshift-data API)
Redshift Serverless (dim_media, dim_visitor, fact_media_engagement)
     |
     v
Amazon QuickSight dashboards
```

Orchestration: **Glue Workflow** with a scheduled trigger (06:00 UTC daily)
and two conditional triggers that only fire on the previous job's success.

## Four Wistia API endpoints used

1. Media Metadata -- `GET /v1/medias/{id}.json` (not paginated)
2. Media Stats -- `GET /v1/stats/medias/{id}.json` (not paginated)
3. Visitors -- `GET /v1/stats/visitors.json?media_id={id}` (paginated)
4. Events -- `GET /v1/stats/events.json?media_id={id}` (paginated; has geo)

Channel (Facebook vs YouTube) is **derived from the video name** since the
API has no channel field.

## Repository Layout

```
wistia-pipeline/
├── README.md
├── .gitignore
├── .github/workflows/ci-cd.yml     GitHub Actions CI/CD
│
├── terraform/                      All AWS infrastructure
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── s3.tf                       3 buckets: raw, processed, scripts
│   ├── iam.tf                      Glue job role + Redshift S3 role
│   ├── secrets.tf                  Wistia token + Redshift password
│   ├── ssm.tf                      Watermark parameters
│   ├── glue.tf                     3 jobs + workflow + triggers + uploads
│   ├── redshift.tf                 Serverless namespace + workgroup
│   ├── cloudwatch.tf               Logs + failure alarm
│   ├── sns.tf                      Alert topic + email subscription
│   ├── terraform.tfvars.example
│   └── terraform.tfvars            (gitignored)
│
├── glue_jobs/
│   ├── ingest/wistia_ingest.py         4 endpoints, pagination, retries
│   ├── transform/bronze_to_silver.py   Cleansing, join, channel derivation
│   └── dq_load/dq_and_load.py          DQ + idempotent Redshift merge
│
├── sql/
│   ├── create_tables.sql           Redshift DDL
│   └── copy_templates.sql          Reference COPY statements
│
└── tests/
    ├── conftest.py                 Stubs awsglue/pyspark/awswrangler
    ├── test_ingest.py              Unit tests for ingestion
    ├── test_transform.py           Unit tests for transform helpers
    └── test_dq.py                  Unit tests for DQ+Load
```

## Prerequisites

* AWS account with admin-equivalent permissions
* AWS CLI v1 or v2 installed and configured with a profile
* Terraform >= 1.5.0
* Python 3.11
* (Optional) GitHub account + `gh` CLI to push the repo

## Local Setup

```bash
# Clone or cd into this folder
cd wistia-pipeline

# Create terraform.tfvars from the example and fill in your values
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# edit terraform/terraform.tfvars - set wistia_api_token, alert_email, aws_profile

# Install test deps and run tests
pip install pytest boto3 requests pandas
pytest tests/ -v
```

## Deploy

```bash
cd terraform

terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

After apply:

1. **Confirm the SNS email subscription** -- AWS sends a confirmation email.
2. **Create Redshift tables** using the Redshift Query Editor v2 in the AWS
   Console, or via the redshift-data API:

   ```bash
   aws redshift-data execute-statement \
       --workgroup-name wistia-analytics-wg \
       --database wistia \
       --sql "$(cat ../sql/create_tables.sql)" \
       --profile globalpartners
   ```

## Run the Pipeline Manually

```bash
aws glue start-workflow-run \
    --name wistia-daily-workflow \
    --profile globalpartners \
    --region us-east-1
```

Monitor:

```bash
aws glue get-workflow-run \
    --name wistia-daily-workflow \
    --run-id <run-id-from-above> \
    --include-graph \
    --profile globalpartners
```

Or watch in the console: **AWS Glue > Workflows > wistia-daily-workflow**.

## Verify Data Landed

```bash
# Bronze should have 4 folders under today's date partition
aws s3 ls s3://wistia-analytics-raw-<acct-id>/ --recursive --profile globalpartners | head

# Silver should have Parquet files for each table
aws s3 ls s3://wistia-analytics-processed-<acct-id>/ --recursive --profile globalpartners | head

# Redshift table counts
aws redshift-data execute-statement \
    --workgroup-name wistia-analytics-wg \
    --database wistia \
    --sql "SELECT COUNT(*) FROM public.fact_media_engagement;" \
    --profile globalpartners
```

## Daily Schedule

The scheduled trigger `wistia-daily-schedule` fires at `cron(0 6 * * ? *)` --
6 AM UTC daily. Change in `terraform/variables.tf` (`schedule_cron`).

## Cleanup (after the 7-day run)

```bash
cd terraform
terraform destroy
```

This removes all infrastructure. S3 buckets have `force_destroy = true` so
they'll be emptied automatically.

## Expected Monthly Cost

Roughly **$5.50 – $12 / month**, dominated by Redshift Serverless
(8 RPU base, charged when queries run) and Glue PySpark DPU-hours.

## Security Notes

* Wistia API token stored in AWS Secrets Manager, never in code
* Redshift admin password auto-generated by Terraform, stored in Secrets Manager
* S3 buckets: SSE-AES256 encryption, all public access blocked
* IAM roles follow least-privilege (Glue role can't touch Redshift admin,
  Redshift role can only read from Silver)

## CI/CD

GitHub Actions runs on every PR and push to main:

* **Lint + Unit tests** run on every PR
* **Terraform validate + plan** on every PR
* **Terraform apply** on push to `main` (guarded by the `production` environment
  in GitHub which can require manual approval)

Required GitHub secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`WISTIA_API_TOKEN`, `ALERT_EMAIL`.
