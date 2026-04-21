# ============================================================
# NOTE: Glue auto-creates /aws-glue/jobs/output and /aws-glue/jobs/error
# log groups the first time any job runs. We do not manage them via
# Terraform (they'd collide on re-apply). Retention can be set manually
# via the AWS CLI once logs start flowing.
# ============================================================

# ============================================================
# EVENTBRIDGE RULE - Catch Glue Workflow failures
# ============================================================
resource "aws_cloudwatch_event_rule" "glue_workflow_failure" {
  name        = "wistia-workflow-failure-rule"
  description = "Fires when the Wistia Glue Workflow run fails"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      state    = ["FAILED", "TIMEOUT", "STOPPED"]
      jobName  = [
        aws_glue_job.ingest.name,
        aws_glue_job.transform.name,
        aws_glue_job.dq_and_load.name
      ]
    }
  })
}

resource "aws_cloudwatch_event_target" "sns" {
  rule      = aws_cloudwatch_event_rule.glue_workflow_failure.name
  target_id = "sns-pipeline-alerts"
  arn       = aws_sns_topic.pipeline_alerts.arn
}

resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn = aws_sns_topic.pipeline_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "SNS:Publish"
      Resource  = aws_sns_topic.pipeline_alerts.arn
    }]
  })
}

# ============================================================
# CLOUDWATCH ALARM - Redshift Serverless idle alarm (optional cost guard)
# ============================================================
resource "aws_cloudwatch_metric_alarm" "glue_job_failures" {
  alarm_name          = "wistia-glue-job-failures"
  alarm_description   = "Alert when any Glue job in the Wistia pipeline fails"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  period              = 300
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}
