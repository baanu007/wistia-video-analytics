# ============================================================
# SSM PARAMETER STORE - High-watermark tracking for incremental loads
# One parameter per media ID, per endpoint type
# ============================================================

# Watermark for visitor endpoint (tracks created_at of latest visitor pulled)
resource "aws_ssm_parameter" "visitor_watermark" {
  for_each = toset(var.media_ids)

  name        = "/wistia/watermark/visitors/${each.value}"
  description = "Last visitor created_at for incremental pulls - media ${each.value}"
  type        = "String"
  value       = "1970-01-01T00:00:00Z"

  lifecycle {
    ignore_changes = [value]  # Updated by Glue job, not Terraform
  }
}

# Watermark for events endpoint (tracks received_at of latest event pulled)
resource "aws_ssm_parameter" "events_watermark" {
  for_each = toset(var.media_ids)

  name        = "/wistia/watermark/events/${each.value}"
  description = "Last event received_at for incremental pulls - media ${each.value}"
  type        = "String"
  value       = "1970-01-01T00:00:00Z"

  lifecycle {
    ignore_changes = [value]
  }
}
