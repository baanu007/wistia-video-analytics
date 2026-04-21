# ============================================================
# REDSHIFT SERVERLESS - Gold layer data warehouse
# ============================================================

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "wistia-analytics-ns"
  admin_username      = var.redshift_admin_username
  admin_user_password = random_password.redshift_admin.result
  db_name             = "wistia"

  iam_roles        = [aws_iam_role.redshift_s3.arn]
  default_iam_role_arn = aws_iam_role.redshift_s3.arn

  log_exports = ["userlog", "connectionlog", "useractivitylog"]
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "wistia-analytics-wg"

  base_capacity      = var.redshift_base_capacity
  publicly_accessible = true  # Needed for redshift-data API from Glue in default VPC

  config_parameter {
    parameter_key   = "enable_user_activity_logging"
    parameter_value = "true"
  }
}
