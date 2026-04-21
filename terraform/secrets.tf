# ============================================================
# WISTIA API TOKEN
# ============================================================
resource "aws_secretsmanager_secret" "wistia_token" {
  name                    = "wistia/api-token"
  description             = "Wistia Stats API Bearer token"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "wistia_token" {
  secret_id = aws_secretsmanager_secret.wistia_token.id
  secret_string = jsonencode({
    api_token = var.wistia_api_token
  })
}

# ============================================================
# REDSHIFT ADMIN PASSWORD (auto-generated)
# ============================================================
resource "random_password" "redshift_admin" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
  min_lower        = 2
  min_upper        = 2
  min_numeric      = 2
  min_special      = 2
}

resource "aws_secretsmanager_secret" "redshift_admin" {
  name                    = "wistia/redshift-admin"
  description             = "Redshift Serverless admin credentials for Wistia analytics"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "redshift_admin" {
  secret_id = aws_secretsmanager_secret.redshift_admin.id
  secret_string = jsonencode({
    username = var.redshift_admin_username
    password = random_password.redshift_admin.result
  })
}
