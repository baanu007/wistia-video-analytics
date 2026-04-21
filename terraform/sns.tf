# ============================================================
# SNS TOPIC - Pipeline alerts
# ============================================================
resource "aws_sns_topic" "pipeline_alerts" {
  name         = "wistia-pipeline-alerts"
  display_name = "Wistia Pipeline Alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
