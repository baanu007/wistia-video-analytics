# ============================================================
# GLUE JOB EXECUTION ROLE
# Single role shared by all 3 Glue jobs (Python Shell + PySpark)
# ============================================================
resource "aws_iam_role" "glue_job" {
  name = "wistia-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_managed" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "wistia-glue-s3-access"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_secrets" {
  name = "wistia-glue-secrets-access"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["secretsmanager:GetSecretValue"]
      Resource = [
        aws_secretsmanager_secret.wistia_token.arn,
        aws_secretsmanager_secret.redshift_admin.arn
      ]
    }]
  })
}

resource "aws_iam_role_policy" "glue_ssm" {
  name = "wistia-glue-ssm-access"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:PutParameter"
      ]
      Resource = "arn:aws:ssm:${var.aws_region}:${local.account_id}:parameter/wistia/watermark/*"
    }]
  })
}

resource "aws_iam_role_policy" "glue_redshift_data" {
  name = "wistia-glue-redshift-data-api"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "redshift-data:ExecuteStatement",
        "redshift-data:BatchExecuteStatement",
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult",
        "redshift-data:ListStatements",
        "redshift-data:CancelStatement",
        "redshift-serverless:GetCredentials"
      ]
      Resource = "*"
    }]
  })
}

# ============================================================
# REDSHIFT ROLE - Assumed by Redshift for S3 COPY access
# ============================================================
resource "aws_iam_role" "redshift_s3" {
  name = "wistia-redshift-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3_read" {
  name = "wistia-redshift-s3-read"
  role = aws_iam_role.redshift_s3.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        aws_s3_bucket.silver.arn,
        "${aws_s3_bucket.silver.arn}/*"
      ]
    }]
  })
}
