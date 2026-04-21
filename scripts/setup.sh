#!/usr/bin/env bash
# ============================================================
# setup.sh -- Provision ALL AWS infrastructure for the Wistia
# Video Analytics pipeline using the AWS CLI.
#
# Idempotent: safe to re-run. Existing resources are left alone.
#
# Prerequisites:
#   - AWS CLI configured (profile with admin-equivalent access)
#   - Env vars:
#       WISTIA_API_TOKEN  (required)
#       ALERT_EMAIL       (optional, defaults to saved memory)
#       AWS_PROFILE       (optional, defaults to 'globalpartners')
#       AWS_REGION        (optional, defaults to 'us-east-1')
#
# Usage:
#   export WISTIA_API_TOKEN=your-bearer-token
#   bash scripts/setup.sh
# ============================================================

# shellcheck source=common.sh
source "$(dirname "$0")/common.sh"

[[ -z "${WISTIA_API_TOKEN:-}" ]] && die "WISTIA_API_TOKEN env var is required."

log "Account: $ACCOUNT_ID  Region: $AWS_REGION  Profile: $AWS_PROFILE"

# ============================================================
# 1. S3 BUCKETS (Bronze / Silver / Scripts)
# ============================================================
log "Step 1/9  S3 buckets"
ensure_bucket "$BUCKET_RAW"
ensure_bucket "$BUCKET_PROC"
ensure_bucket "$BUCKET_SCRIPTS"

# Lifecycle on Bronze: IA after 90d, Glacier after 365d
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET_RAW" \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "archive-old-raw-data",
      "Status": "Enabled",
      "Filter": {},
      "Transitions": [
        { "Days": 90,  "StorageClass": "STANDARD_IA" },
        { "Days": 365, "StorageClass": "GLACIER"     }
      ],
      "NoncurrentVersionExpiration": { "NoncurrentDays": 30 }
    }]
  }' >/dev/null
ok "Bronze lifecycle rule applied"

# ============================================================
# 2. SECRETS (Wistia API token + Redshift admin password)
# ============================================================
log "Step 2/9  Secrets Manager"
ensure_secret_if_missing "$WISTIA_SECRET_NAME" \
  "$(printf '{"api_token":"%s"}' "$WISTIA_API_TOKEN")"

if ! aws secretsmanager describe-secret --secret-id "$REDSHIFT_SECRET_NAME" >/dev/null 2>&1; then
  log "Generating Redshift admin password"
  REDSHIFT_PASSWORD=$(openssl rand -base64 24 | tr -d '=+/' | cut -c1-24)
  ensure_secret_if_missing "$REDSHIFT_SECRET_NAME" \
    "$(printf '{"username":"%s","password":"%s"}' "$REDSHIFT_ADMIN_USER" "$REDSHIFT_PASSWORD")"
else
  skip "Redshift admin secret already exists (not rotating)"
  REDSHIFT_PASSWORD=$(aws secretsmanager get-secret-value --secret-id "$REDSHIFT_SECRET_NAME" \
    --query SecretString --output text | python -c 'import json,sys; print(json.load(sys.stdin)["password"])')
fi

WISTIA_SECRET_ARN=$(secret_arn "$WISTIA_SECRET_NAME")
REDSHIFT_SECRET_ARN=$(secret_arn "$REDSHIFT_SECRET_NAME")

# ============================================================
# 3. IAM ROLES + POLICIES
# ============================================================
log "Step 3/9  IAM roles"
ensure_role "$GLUE_ROLE_NAME" "$IAM_DIR/glue-trust.json"
attach_managed_policy "$GLUE_ROLE_NAME" "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
put_inline_policy "$GLUE_ROLE_NAME" "wistia-glue-permissions" "$IAM_DIR/glue-permissions.json"

ensure_role "$REDSHIFT_ROLE_NAME" "$IAM_DIR/redshift-trust.json"
put_inline_policy "$REDSHIFT_ROLE_NAME" "wistia-redshift-s3-read" "$IAM_DIR/redshift-s3-read.json"

GLUE_ROLE_ARN=$(aws iam get-role --role-name "$GLUE_ROLE_NAME" --query 'Role.Arn' --output text)
REDSHIFT_ROLE_ARN=$(aws iam get-role --role-name "$REDSHIFT_ROLE_NAME" --query 'Role.Arn' --output text)

# Small sleep to let IAM role propagation catch up on first create
sleep 5

# ============================================================
# 4. SSM WATERMARK PARAMETERS
# ============================================================
log "Step 4/9  SSM watermark parameters"
IFS=',' read -ra MIDS <<< "$MEDIA_IDS"
for mid in "${MIDS[@]}"; do
  ensure_ssm_param "/wistia/watermark/visitors/$mid" \
    "Last visitor created_at for incremental pulls - media $mid" \
    "1970-01-01T00:00:00Z"
  ensure_ssm_param "/wistia/watermark/events/$mid" \
    "Last event received_at for incremental pulls - media $mid" \
    "1970-01-01T00:00:00Z"
done

# ============================================================
# 5. SNS TOPIC + EMAIL SUBSCRIPTION
# ============================================================
log "Step 5/9  SNS alerts"
SNS_TOPIC_ARN=$(ensure_sns_topic "$SNS_TOPIC_NAME")
ok "SNS topic ARN: $SNS_TOPIC_ARN"
ensure_sns_email_subscription "$SNS_TOPIC_ARN" "$ALERT_EMAIL"

# Topic policy: allow EventBridge to publish
aws sns set-topic-attributes --topic-arn "$SNS_TOPIC_ARN" --attribute-name Policy \
  --attribute-value "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "events.amazonaws.com" },
    "Action": "SNS:Publish",
    "Resource": "$SNS_TOPIC_ARN"
  }]
}
EOF
)" >/dev/null
ok "SNS topic policy allows EventBridge"

# ============================================================
# 6. REDSHIFT SERVERLESS (namespace + workgroup)
# ============================================================
log "Step 6/9  Redshift Serverless"

if aws redshift-serverless get-namespace --namespace-name "$REDSHIFT_NAMESPACE" >/dev/null 2>&1; then
  skip "Redshift namespace $REDSHIFT_NAMESPACE already exists"
else
  log "Creating Redshift namespace (takes ~30s)"
  aws redshift-serverless create-namespace \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --admin-username "$REDSHIFT_ADMIN_USER" \
    --admin-user-password "$REDSHIFT_PASSWORD" \
    --db-name "$REDSHIFT_DB" \
    --iam-roles "$REDSHIFT_ROLE_ARN" \
    --default-iam-role-arn "$REDSHIFT_ROLE_ARN" \
    --log-exports userlog connectionlog useractivitylog >/dev/null
  ok "Namespace created"
fi

if aws redshift-serverless get-workgroup --workgroup-name "$REDSHIFT_WORKGROUP" >/dev/null 2>&1; then
  skip "Redshift workgroup $REDSHIFT_WORKGROUP already exists"
else
  log "Creating Redshift workgroup (takes 1-2 minutes)"
  aws redshift-serverless create-workgroup \
    --workgroup-name "$REDSHIFT_WORKGROUP" \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --base-capacity "$REDSHIFT_BASE_CAPACITY" \
    --publicly-accessible \
    --config-parameters "parameterKey=enable_user_activity_logging,parameterValue=true" \
    >/dev/null
  ok "Workgroup creation started; waiting..."
  while true; do
    STATUS=$(aws redshift-serverless get-workgroup --workgroup-name "$REDSHIFT_WORKGROUP" \
      --query 'workgroup.status' --output text 2>/dev/null || echo "CREATING")
    [[ "$STATUS" == "AVAILABLE" ]] && break
    printf '.'; sleep 10
  done
  echo
  ok "Workgroup AVAILABLE"
fi

# ============================================================
# 7. UPLOAD GLUE SCRIPTS TO S3
# ============================================================
log "Step 7/9  Upload Glue scripts"
aws s3 cp "$(winpath "$REPO_ROOT/glue_jobs/ingest/wistia_ingest.py")" \
          "s3://$BUCKET_SCRIPTS/glue_jobs/ingest/wistia_ingest.py" >/dev/null
aws s3 cp "$(winpath "$REPO_ROOT/glue_jobs/transform/bronze_to_silver.py")" \
          "s3://$BUCKET_SCRIPTS/glue_jobs/transform/bronze_to_silver.py" >/dev/null
aws s3 cp "$(winpath "$REPO_ROOT/glue_jobs/dq_load/dq_and_load.py")" \
          "s3://$BUCKET_SCRIPTS/glue_jobs/dq_load/dq_and_load.py" >/dev/null
ok "3 scripts uploaded"

# ============================================================
# 8. GLUE JOBS (ingest / transform / dq_and_load)
# ============================================================
log "Step 8/9  Glue jobs"

# Build the create-job JSON payloads (rendered per job).
# Use a tempdir inside the repo so Windows AWS CLI can resolve file:// paths.
TMP_JSON="$SCRIPT_DIR/.tmp_payloads_$$"
mkdir -p "$TMP_JSON"
trap 'rm -rf "$TMP_JSON"' EXIT

cat > "$TMP_JSON/ingest.json" <<EOF
{
  "Name": "$GLUE_JOB_INGEST",
  "Description": "Pull 4 Wistia API endpoints for both media IDs; write raw JSON to Bronze",
  "Role": "$GLUE_ROLE_ARN",
  "GlueVersion": "3.0",
  "MaxRetries": 0,
  "Timeout": 240,
  "MaxCapacity": 1.0,
  "Command": {
    "Name": "pythonshell",
    "PythonVersion": "3.9",
    "ScriptLocation": "s3://$BUCKET_SCRIPTS/glue_jobs/ingest/wistia_ingest.py"
  },
  "DefaultArguments": {
    "--job-language": "python",
    "--TempDir": "s3://$BUCKET_SCRIPTS/temp/",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-metrics": "true",
    "--BRONZE_BUCKET": "$BUCKET_RAW",
    "--WISTIA_SECRET_NAME": "$WISTIA_SECRET_NAME",
    "--MEDIA_IDS": "$MEDIA_IDS",
    "--AWS_REGION_NAME": "$AWS_REGION",
    "--additional-python-modules": "requests==2.32.3"
  }
}
EOF

cat > "$TMP_JSON/transform.json" <<EOF
{
  "Name": "$GLUE_JOB_TRANSFORM",
  "Description": "PySpark: Bronze JSON -> clean joined Silver Parquet with derived channel",
  "Role": "$GLUE_ROLE_ARN",
  "GlueVersion": "4.0",
  "MaxRetries": 0,
  "Timeout": 30,
  "WorkerType": "G.1X",
  "NumberOfWorkers": 2,
  "Command": {
    "Name": "glueetl",
    "PythonVersion": "3",
    "ScriptLocation": "s3://$BUCKET_SCRIPTS/glue_jobs/transform/bronze_to_silver.py"
  },
  "DefaultArguments": {
    "--job-language": "python",
    "--job-bookmark-option": "job-bookmark-disable",
    "--TempDir": "s3://$BUCKET_SCRIPTS/temp/",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-metrics": "true",
    "--enable-spark-ui": "false",
    "--BRONZE_BUCKET": "$BUCKET_RAW",
    "--SILVER_BUCKET": "$BUCKET_PROC"
  }
}
EOF

cat > "$TMP_JSON/dqload.json" <<EOF
{
  "Name": "$GLUE_JOB_DQLOAD",
  "Description": "DQ checks on Silver Parquet + Redshift COPY-and-merge",
  "Role": "$GLUE_ROLE_ARN",
  "GlueVersion": "3.0",
  "MaxRetries": 0,
  "Timeout": 20,
  "MaxCapacity": 1.0,
  "Command": {
    "Name": "pythonshell",
    "PythonVersion": "3.9",
    "ScriptLocation": "s3://$BUCKET_SCRIPTS/glue_jobs/dq_load/dq_and_load.py"
  },
  "DefaultArguments": {
    "--job-language": "python",
    "--TempDir": "s3://$BUCKET_SCRIPTS/temp/",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-metrics": "true",
    "--SILVER_BUCKET": "$BUCKET_PROC",
    "--REDSHIFT_WORKGROUP": "$REDSHIFT_WORKGROUP",
    "--REDSHIFT_DATABASE": "$REDSHIFT_DB",
    "--REDSHIFT_IAM_ROLE_ARN": "$REDSHIFT_ROLE_ARN",
    "--REDSHIFT_SECRET_ARN": "$REDSHIFT_SECRET_ARN",
    "--AWS_REGION_NAME": "$AWS_REGION",
    "--additional-python-modules": "boto3>=1.29.0,botocore>=1.32.0"
  }
}
EOF

upsert_glue_job "$GLUE_JOB_INGEST"    "$TMP_JSON/ingest.json"
upsert_glue_job "$GLUE_JOB_TRANSFORM" "$TMP_JSON/transform.json"
upsert_glue_job "$GLUE_JOB_DQLOAD"    "$TMP_JSON/dqload.json"

# ============================================================
# 9. GLUE WORKFLOW + TRIGGERS + EventBridge failure alerting
# ============================================================
log "Step 9/9  Glue workflow, triggers, EventBridge failure rule"

ensure_glue_workflow "$GLUE_WORKFLOW" \
  "Daily Wistia analytics ingest -> transform -> DQ+load pipeline"

# Trigger 1: SCHEDULED (kicks off ingest at the cron)
cat > "$TMP_JSON/trig-sched.json" <<EOF
{
  "Name": "wistia-daily-schedule",
  "Type": "SCHEDULED",
  "Schedule": "$SCHEDULE_CRON",
  "WorkflowName": "$GLUE_WORKFLOW",
  "Actions": [{ "JobName": "$GLUE_JOB_INGEST" }],
  "StartOnCreation": true
}
EOF
ensure_glue_trigger "wistia-daily-schedule" "$TMP_JSON/trig-sched.json"

# Trigger 2: CONDITIONAL (ingest SUCCEEDED -> transform)
cat > "$TMP_JSON/trig-after-ingest.json" <<EOF
{
  "Name": "wistia-after-ingest",
  "Type": "CONDITIONAL",
  "WorkflowName": "$GLUE_WORKFLOW",
  "Predicate": {
    "Conditions": [{
      "LogicalOperator": "EQUALS",
      "JobName": "$GLUE_JOB_INGEST",
      "State": "SUCCEEDED"
    }]
  },
  "Actions": [{ "JobName": "$GLUE_JOB_TRANSFORM" }],
  "StartOnCreation": true
}
EOF
ensure_glue_trigger "wistia-after-ingest" "$TMP_JSON/trig-after-ingest.json"

# Trigger 3: CONDITIONAL (transform SUCCEEDED -> dq_and_load)
cat > "$TMP_JSON/trig-after-transform.json" <<EOF
{
  "Name": "wistia-after-transform",
  "Type": "CONDITIONAL",
  "WorkflowName": "$GLUE_WORKFLOW",
  "Predicate": {
    "Conditions": [{
      "LogicalOperator": "EQUALS",
      "JobName": "$GLUE_JOB_TRANSFORM",
      "State": "SUCCEEDED"
    }]
  },
  "Actions": [{ "JobName": "$GLUE_JOB_DQLOAD" }],
  "StartOnCreation": true
}
EOF
ensure_glue_trigger "wistia-after-transform" "$TMP_JSON/trig-after-transform.json"

# EventBridge rule: catch Glue job FAILED/TIMEOUT/STOPPED -> SNS
RULE_NAME="wistia-workflow-failure-rule"
if aws events describe-rule --name "$RULE_NAME" >/dev/null 2>&1; then
  skip "EventBridge rule $RULE_NAME already exists"
else
  log "Creating EventBridge rule $RULE_NAME"
  aws events put-rule --name "$RULE_NAME" --state ENABLED \
    --description "Fire when any Wistia Glue job changes to FAILED/TIMEOUT/STOPPED" \
    --event-pattern "$(cat <<EOF
{
  "source": ["aws.glue"],
  "detail-type": ["Glue Job State Change"],
  "detail": {
    "state": ["FAILED", "TIMEOUT", "STOPPED"],
    "jobName": ["$GLUE_JOB_INGEST", "$GLUE_JOB_TRANSFORM", "$GLUE_JOB_DQLOAD"]
  }
}
EOF
)" >/dev/null
  aws events put-targets --rule "$RULE_NAME" \
    --targets "Id=sns-pipeline-alerts,Arn=$SNS_TOPIC_ARN" >/dev/null
  ok "EventBridge rule + SNS target configured"
fi

# CloudWatch alarm on Glue aggregate failed tasks
aws cloudwatch put-metric-alarm \
  --alarm-name "wistia-glue-job-failures" \
  --alarm-description "Alert when any Wistia Glue job has failed tasks" \
  --namespace "Glue" \
  --metric-name "glue.driver.aggregate.numFailedTasks" \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$SNS_TOPIC_ARN" >/dev/null
ok "CloudWatch alarm configured"

rm -rf "$TMP_JSON"

log "DONE. All infrastructure provisioned."
echo
echo "Next steps:"
echo "  1. Confirm the SNS subscription email sent to $ALERT_EMAIL"
echo "  2. Create Redshift tables:    bash scripts/create-tables.sh"
echo "  3. Trigger a manual run:      aws glue start-workflow-run --name $GLUE_WORKFLOW"
