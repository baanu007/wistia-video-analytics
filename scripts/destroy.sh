#!/usr/bin/env bash
# ============================================================
# destroy.sh -- Tear down ALL Wistia Video Analytics AWS
# infrastructure, in reverse dependency order.
#
# SAFETY: This deletes data. Requires typing 'DESTROY' to confirm.
#
# What gets deleted:
#   - Glue triggers, workflow, jobs
#   - EventBridge rule + SNS topic + CloudWatch alarm
#   - Redshift Serverless workgroup + namespace
#   - Secrets (Wistia token + Redshift admin password)
#   - SSM parameters (watermarks)
#   - IAM roles + inline policies
#   - S3 buckets (Bronze, Silver, Scripts) -- emptied then deleted
#
# What STAYS (intentionally):
#   - Nothing. Full teardown.
# ============================================================

# shellcheck source=common.sh
source "$(dirname "$0")/common.sh"

cat <<WARN
============================================================
  WARNING: This will PERMANENTLY DELETE all Wistia Video
  Analytics infrastructure in account $ACCOUNT_ID ($AWS_REGION):

    - 3 S3 buckets AND ALL THEIR DATA
    - Redshift Serverless workgroup + namespace (and tables)
    - Glue jobs, workflow, triggers
    - IAM roles, Secrets, SSM parameters
    - SNS topic + subscriptions
    - CloudWatch alarm + EventBridge rule

  This cannot be undone.
============================================================
WARN
read -r -p "Type DESTROY to continue: " confirm
[[ "$confirm" == "DESTROY" ]] || die "Aborted."

# ------------------------------------------------------------
# 1. Glue triggers -> workflow -> jobs
# ------------------------------------------------------------
log "Deleting Glue triggers"
for t in wistia-after-transform wistia-after-ingest wistia-daily-schedule; do
  if aws glue get-trigger --name "$t" >/dev/null 2>&1; then
    aws glue stop-trigger --name "$t" >/dev/null 2>&1 || true
    aws glue delete-trigger --name "$t" >/dev/null && ok "trigger $t deleted"
  fi
done

log "Deleting Glue workflow"
aws glue delete-workflow --name "$GLUE_WORKFLOW" >/dev/null 2>&1 && ok "workflow deleted" || skip "no workflow"

log "Deleting Glue jobs"
for j in "$GLUE_JOB_DQLOAD" "$GLUE_JOB_TRANSFORM" "$GLUE_JOB_INGEST"; do
  aws glue delete-job --job-name "$j" >/dev/null 2>&1 && ok "job $j deleted" || skip "no $j"
done

# ------------------------------------------------------------
# 2. EventBridge rule + CloudWatch alarm
# ------------------------------------------------------------
log "Deleting EventBridge rule + CloudWatch alarm"
RULE_NAME="wistia-workflow-failure-rule"
aws events remove-targets --rule "$RULE_NAME" --ids sns-pipeline-alerts >/dev/null 2>&1 || true
aws events delete-rule --name "$RULE_NAME" >/dev/null 2>&1 && ok "rule deleted" || skip "no rule"
aws cloudwatch delete-alarms --alarm-names "wistia-glue-job-failures" >/dev/null 2>&1 && ok "alarm deleted" || skip "no alarm"

# ------------------------------------------------------------
# 3. Redshift Serverless workgroup then namespace
# ------------------------------------------------------------
log "Deleting Redshift workgroup (takes ~1 min)"
if aws redshift-serverless get-workgroup --workgroup-name "$REDSHIFT_WORKGROUP" >/dev/null 2>&1; then
  aws redshift-serverless delete-workgroup --workgroup-name "$REDSHIFT_WORKGROUP" >/dev/null
  while aws redshift-serverless get-workgroup --workgroup-name "$REDSHIFT_WORKGROUP" >/dev/null 2>&1; do
    printf '.'; sleep 10
  done
  echo
  ok "workgroup deleted"
else
  skip "no workgroup"
fi

log "Deleting Redshift namespace"
if aws redshift-serverless get-namespace --namespace-name "$REDSHIFT_NAMESPACE" >/dev/null 2>&1; then
  aws redshift-serverless delete-namespace --namespace-name "$REDSHIFT_NAMESPACE" \
    --no-final-snapshot-name >/dev/null 2>&1 || \
    aws redshift-serverless delete-namespace --namespace-name "$REDSHIFT_NAMESPACE" >/dev/null
  ok "namespace deleted"
else
  skip "no namespace"
fi

# ------------------------------------------------------------
# 4. SNS topic
# ------------------------------------------------------------
log "Deleting SNS topic"
SNS_TOPIC_ARN="arn:aws:sns:$AWS_REGION:$ACCOUNT_ID:$SNS_TOPIC_NAME"
aws sns delete-topic --topic-arn "$SNS_TOPIC_ARN" >/dev/null 2>&1 && ok "topic deleted" || skip "no topic"

# ------------------------------------------------------------
# 5. Secrets Manager (force delete -- no recovery window)
# ------------------------------------------------------------
log "Deleting Secrets"
for s in "$WISTIA_SECRET_NAME" "$REDSHIFT_SECRET_NAME"; do
  aws secretsmanager delete-secret --secret-id "$s" --force-delete-without-recovery >/dev/null 2>&1 \
    && ok "secret $s deleted" || skip "no $s"
done

# ------------------------------------------------------------
# 6. SSM watermark parameters
# ------------------------------------------------------------
log "Deleting SSM watermark parameters"
IFS=',' read -ra MIDS <<< "$MEDIA_IDS"
for mid in "${MIDS[@]}"; do
  for endpoint in visitors events; do
    aws ssm delete-parameter --name "/wistia/watermark/$endpoint/$mid" >/dev/null 2>&1 \
      && ok "watermark $endpoint/$mid deleted" || true
  done
done

# ------------------------------------------------------------
# 7. IAM roles (detach policies first)
# ------------------------------------------------------------
log "Deleting IAM roles"
for role in "$GLUE_ROLE_NAME" "$REDSHIFT_ROLE_NAME"; do
  if aws iam get-role --role-name "$role" >/dev/null 2>&1; then
    # Detach managed policies
    for arn in $(aws iam list-attached-role-policies --role-name "$role" \
                   --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do
      aws iam detach-role-policy --role-name "$role" --policy-arn "$arn" >/dev/null 2>&1 || true
    done
    # Delete inline policies
    for p in $(aws iam list-role-policies --role-name "$role" \
                 --query 'PolicyNames' --output text 2>/dev/null); do
      aws iam delete-role-policy --role-name "$role" --policy-name "$p" >/dev/null 2>&1 || true
    done
    aws iam delete-role --role-name "$role" >/dev/null
    ok "role $role deleted"
  else
    skip "no $role"
  fi
done

# ------------------------------------------------------------
# 8. S3 buckets -- empty then delete
# ------------------------------------------------------------
log "Emptying and deleting S3 buckets"
for b in "$BUCKET_RAW" "$BUCKET_PROC" "$BUCKET_SCRIPTS"; do
  if aws s3api head-bucket --bucket "$b" 2>/dev/null; then
    log "  emptying $b (including all versions)..."
    aws s3api delete-objects --bucket "$b" \
      --delete "$(aws s3api list-object-versions --bucket "$b" \
        --query '{Objects: Versions[].{Key: Key, VersionId: VersionId}}' --output json 2>/dev/null)" \
      >/dev/null 2>&1 || true
    aws s3api delete-objects --bucket "$b" \
      --delete "$(aws s3api list-object-versions --bucket "$b" \
        --query '{Objects: DeleteMarkers[].{Key: Key, VersionId: VersionId}}' --output json 2>/dev/null)" \
      >/dev/null 2>&1 || true
    aws s3 rm "s3://$b" --recursive >/dev/null 2>&1 || true
    aws s3api delete-bucket --bucket "$b" >/dev/null && ok "bucket $b deleted"
  else
    skip "no $b"
  fi
done

log "DONE. All Wistia infrastructure destroyed."
