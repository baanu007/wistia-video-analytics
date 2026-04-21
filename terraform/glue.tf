# ============================================================
# UPLOAD GLUE SCRIPTS TO S3
# Terraform uploads the local Python files into the scripts bucket.
# Glue jobs reference these S3 paths as their --scriptLocation.
# ============================================================

resource "aws_s3_object" "script_ingest" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/ingest/wistia_ingest.py"
  source = "${path.module}/../glue_jobs/ingest/wistia_ingest.py"
  etag   = filemd5("${path.module}/../glue_jobs/ingest/wistia_ingest.py")
}

resource "aws_s3_object" "script_transform" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/transform/bronze_to_silver.py"
  source = "${path.module}/../glue_jobs/transform/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../glue_jobs/transform/bronze_to_silver.py")
}

resource "aws_s3_object" "script_dq_load" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/dq_load/dq_and_load.py"
  source = "${path.module}/../glue_jobs/dq_load/dq_and_load.py"
  etag   = filemd5("${path.module}/../glue_jobs/dq_load/dq_and_load.py")
}

# ============================================================
# GLUE JOB 1: INGEST (Python Shell)
# Hits 4 Wistia API endpoints for both media IDs, writes to Bronze S3
# ============================================================
resource "aws_glue_job" "ingest" {
  name              = "wistia-ingest"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "3.0"   # Python Shell 3.9
  max_retries       = 0
  # Day-1 does a full backfill of all visitor + event pages for both media IDs.
  # That can take several hours. Days 2+ short-circuit on the watermark and
  # complete in minutes. Set a generous timeout that covers day-1.
  timeout           = 240     # minutes
  max_capacity      = 1.0

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.script_ingest.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.id}/temp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--BRONZE_BUCKET"                    = aws_s3_bucket.bronze.id
    "--WISTIA_SECRET_NAME"               = aws_secretsmanager_secret.wistia_token.name
    "--MEDIA_IDS"                        = join(",", var.media_ids)
    "--AWS_REGION_NAME"                  = var.aws_region
    "--additional-python-modules"        = "requests==2.32.3"
  }

  depends_on = [aws_s3_object.script_ingest]
}

# ============================================================
# GLUE JOB 2: TRANSFORM (PySpark)
# Reads Bronze JSON, cleans/joins/derives, writes Silver Parquet
# ============================================================
resource "aws_glue_job" "transform" {
  name              = "wistia-transform"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  max_retries       = 0
  timeout           = 30      # minutes
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.script_transform.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.id}/temp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "false"
    "--BRONZE_BUCKET"                    = aws_s3_bucket.bronze.id
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.id
  }

  depends_on = [aws_s3_object.script_transform]
}

# ============================================================
# GLUE JOB 3: DQ CHECK + REDSHIFT LOAD (Python Shell)
# Validates Silver Parquet, then COPYs into Redshift
# ============================================================
resource "aws_glue_job" "dq_and_load" {
  name              = "wistia-dq-and-load"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "3.0"
  max_retries       = 0
  timeout           = 20
  max_capacity      = 1.0

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.script_dq_load.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.id}/temp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.id
    "--REDSHIFT_WORKGROUP"               = aws_redshiftserverless_workgroup.main.workgroup_name
    "--REDSHIFT_DATABASE"                = aws_redshiftserverless_namespace.main.db_name
    "--REDSHIFT_IAM_ROLE_ARN"            = aws_iam_role.redshift_s3.arn
    "--REDSHIFT_SECRET_ARN"              = aws_secretsmanager_secret.redshift_admin.arn
    "--AWS_REGION_NAME"                  = var.aws_region
    # Glue Python Shell 3.9 ships an old boto3 that pre-dates the Redshift
    # Serverless `WorkgroupName` parameter on execute_statement. Upgrade.
    "--additional-python-modules"        = "boto3>=1.29.0,botocore>=1.32.0"
  }

  depends_on = [aws_s3_object.script_dq_load]
}

# ============================================================
# GLUE WORKFLOW - Chains the 3 jobs sequentially
# ============================================================
resource "aws_glue_workflow" "daily" {
  name        = "wistia-daily-workflow"
  description = "Daily Wistia analytics ingest -> transform -> DQ+load pipeline"

  default_run_properties = {
    "run_started_by" = "glue-workflow-trigger"
  }
}

# Trigger 1: SCHEDULED - kicks off the workflow at 06:00 UTC daily
resource "aws_glue_trigger" "scheduled" {
  name          = "wistia-daily-schedule"
  type          = "SCHEDULED"
  schedule      = var.schedule_cron
  workflow_name = aws_glue_workflow.daily.name
  enabled       = true

  actions {
    job_name = aws_glue_job.ingest.name
  }
}

# Trigger 2: CONDITIONAL - runs transform only if ingest SUCCEEDED
resource "aws_glue_trigger" "after_ingest" {
  name          = "wistia-after-ingest"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.daily.name
  enabled       = true

  predicate {
    conditions {
      job_name = aws_glue_job.ingest.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.transform.name
  }
}

# Trigger 3: CONDITIONAL - runs DQ+load only if transform SUCCEEDED
resource "aws_glue_trigger" "after_transform" {
  name          = "wistia-after-transform"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.daily.name
  enabled       = true

  predicate {
    conditions {
      job_name = aws_glue_job.transform.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.dq_and_load.name
  }
}
