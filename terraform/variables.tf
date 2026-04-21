variable "aws_region" {
  description = "AWS region to deploy to"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Local AWS CLI profile name"
  type        = string
  default     = "globalpartners"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

variable "wistia_api_token" {
  description = "Wistia API Bearer token (sensitive)"
  type        = string
  sensitive   = true
}

variable "media_ids" {
  description = "Wistia media IDs to track"
  type        = list(string)
  default     = ["gskhw4w4lm", "v08dlrgr7v"]
}

variable "alert_email" {
  description = "Email address for SNS pipeline alerts"
  type        = string
}

variable "redshift_admin_username" {
  description = "Redshift Serverless admin username"
  type        = string
  default     = "wistia_admin"
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base capacity (RPU). Minimum 8."
  type        = number
  default     = 8
}

variable "schedule_cron" {
  description = "Cron expression for daily pipeline trigger (UTC)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}
