# Wistia Video Analytics Pipeline

End-to-end data pipeline on AWS that pulls video analytics from the Wistia
Stats API, lands them in S3, transforms to a star schema with PySpark on Glue,
and loads into Redshift Serverless. Orchestrated by AWS Glue Workflows.
All infrastructure provisioned via AWS CLI shell scripts.

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
├── requirements.txt
├── .github/workflows/ci-cd.yml     GitHub Actions CI/CD
│
├── scripts/                        AWS infrastructure as shell scripts
│   ├── common.sh                   Shared env vars + idempotent helpers
│   ├── setup.sh                    Provision all AWS resources (one-shot)
│   ├── deploy-code.sh              Upload updated Glue scripts (fast path)
│   ├── create-tables.sh            Run sql/create_tables.sql on Redshift
│   ├── destroy.sh                  Tear down everything
│   └── iam/                        IAM policy JSON documents
│       ├── glue-trust.json
│       ├── glue-permissions.json
│       ├── redshift-trust.json
│       └── redshift-s3-read.json
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
* Python 3.11
* `openssl` and `bash` (both ship with Git Bash / WSL / macOS / Linux)
* (Optional) GitHub account + `gh` CLI to push the repo

## Environment Variables

The scripts pick up these env vars (all optional except `WISTIA_API_TOKEN`):

| Variable | Default | Purpose |
|---|---|---|
| `WISTIA_API_TOKEN` | *required* | Wistia API Bearer token, stored in Secrets Manager at setup time |
| `ALERT_EMAIL` | `mrmanndy007.mm@gmail.com` | SNS alert target |
| `AWS_PROFILE` | `globalpartners` | Local AWS CLI profile |
| `AWS_REGION` | `us-east-1` | Deployment region |
| `SCHEDULE_CRON` | `cron(0 6 * * ? *)` | Daily pipeline run time |
| `REDSHIFT_BASE_CAPACITY` | `8` | Redshift Serverless base RPU |

## First-Time Setup

```bash
# From the repo root
cd wistia-pipeline

# Install test deps (for local development)
pip install -r requirements.txt
pytest tests/ -v

# One-shot infrastructure provisioning
export WISTIA_API_TOKEN="your-bearer-token-here"
bash scripts/setup.sh

# Create Redshift tables (star schema + staging + views)
bash scripts/create-tables.sh
```

After `setup.sh` completes:
1. Confirm the SNS subscription email sent to `ALERT_EMAIL`
2. Trigger a manual workflow run:
   ```bash
   aws glue start-workflow-run --name wistia-daily-workflow --profile globalpartners
   ```

## Everyday Deploys

After changing any Glue script, re-upload with:

```bash
bash scripts/deploy-code.sh
```

This is the same command CI runs on every push to `main` -- you rarely need
to run it manually unless testing a change before committing.

## Monitor Pipeline Runs

```bash
# List recent runs
aws glue get-workflow-runs --name wistia-daily-workflow --profile globalpartners \
  --query 'Runs[*].[StartedOn,Status]' --output table

# Manual trigger
aws glue start-workflow-run --name wistia-daily-workflow --profile globalpartners

# Tail logs
aws logs tail /aws-glue/python-jobs/output --follow --profile globalpartners
```

Or watch via the AWS Console: **AWS Glue > Workflows > wistia-daily-workflow**.

## Verify Data Landed

```bash
# Bronze (raw JSON)
aws s3 ls s3://wistia-analytics-raw-<acct-id>/ --recursive --profile globalpartners | head

# Silver (Parquet)
aws s3 ls s3://wistia-analytics-processed-<acct-id>/ --recursive --profile globalpartners | head

# Redshift row counts
aws redshift-data execute-statement \
    --workgroup-name wistia-analytics-wg \
    --database wistia \
    --sql "SELECT COUNT(*) FROM public.fact_media_engagement;" \
    --profile globalpartners
```

## Teardown

```bash
bash scripts/destroy.sh
# Type DESTROY to confirm
```

This deletes every AWS resource created by `setup.sh`, including
S3 buckets and their data. Cannot be undone.

## Cost

Roughly **$5.50 – $12 / month**, dominated by Redshift Serverless
(8 RPU base, charged when queries run) and Glue PySpark DPU-hours.

## Security Notes

* Wistia API token stored in AWS Secrets Manager, never in code
* Redshift admin password auto-generated by `setup.sh` (`openssl rand`), stored in Secrets Manager
* S3 buckets: SSE-AES256 encryption, all public access blocked
* IAM roles follow least-privilege (Glue role scoped to specific buckets, secrets, parameters; Redshift role read-only on Silver)

## CI/CD

GitHub Actions runs on every PR and push to `main`:

* **Lint + Unit tests** run on every PR
* **Shell script syntax check** on every PR
* **Deploy Glue scripts** on push to `main` (via `bash scripts/deploy-code.sh`, gated by the `production` environment)

Required GitHub secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
(`WISTIA_API_TOKEN` and `ALERT_EMAIL` are only needed at initial `setup.sh` time,
not on every code deploy.)

Infrastructure changes are **manual**: an admin runs `bash scripts/setup.sh`
from a workstation with the correct AWS profile. This matches the real-world
cadence where code changes happen often but infra rarely.
