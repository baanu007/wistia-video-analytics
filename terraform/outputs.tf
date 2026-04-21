output "account_id" {
  value       = local.account_id
  description = "AWS account ID"
}

output "region" {
  value       = var.aws_region
  description = "AWS region"
}

output "bronze_bucket" {
  value       = aws_s3_bucket.bronze.id
  description = "S3 bucket for raw JSON (Bronze layer)"
}

output "silver_bucket" {
  value       = aws_s3_bucket.silver.id
  description = "S3 bucket for Parquet (Silver layer)"
}

output "scripts_bucket" {
  value       = aws_s3_bucket.scripts.id
  description = "S3 bucket for Glue scripts"
}

output "glue_workflow_name" {
  value       = aws_glue_workflow.daily.name
  description = "Glue Workflow name"
}

output "glue_job_names" {
  value = {
    ingest    = aws_glue_job.ingest.name
    transform = aws_glue_job.transform.name
    dq_load   = aws_glue_job.dq_and_load.name
  }
  description = "Glue job names"
}

output "redshift_workgroup" {
  value       = aws_redshiftserverless_workgroup.main.workgroup_name
  description = "Redshift Serverless workgroup"
}

output "redshift_database" {
  value       = aws_redshiftserverless_namespace.main.db_name
  description = "Redshift database name"
}

output "redshift_endpoint" {
  value       = aws_redshiftserverless_workgroup.main.endpoint
  description = "Redshift Serverless endpoint"
  sensitive   = true
}

output "redshift_admin_secret_arn" {
  value       = aws_secretsmanager_secret.redshift_admin.arn
  description = "ARN of the Secrets Manager secret with Redshift admin credentials"
}

output "wistia_token_secret_arn" {
  value       = aws_secretsmanager_secret.wistia_token.arn
  description = "ARN of the Secrets Manager secret with Wistia API token"
}

output "sns_topic_arn" {
  value       = aws_sns_topic.pipeline_alerts.arn
  description = "SNS topic ARN for pipeline alerts"
}

output "manual_workflow_run_command" {
  value       = "aws glue start-workflow-run --name ${aws_glue_workflow.daily.name} --profile ${var.aws_profile} --region ${var.aws_region}"
  description = "Command to manually trigger the Glue Workflow"
}
