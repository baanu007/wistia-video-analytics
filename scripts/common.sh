#!/usr/bin/env bash
# ============================================================
# common.sh -- shared config + idempotency helpers for the
# Wistia Video Analytics AWS CLI deployment scripts.
#
# This file is sourced by setup.sh, deploy-code.sh, destroy.sh,
# and create-tables.sh. It must never be executed directly.
# ============================================================

set -euo pipefail

# Disable MSYS path conversion so paths starting with "/" are not
# mangled into "C:/Program Files/Git/..." by Git Bash on Windows.
export MSYS_NO_PATHCONV=1

# ------------------------------------------------------------
# Region / profile
# ------------------------------------------------------------
export AWS_REGION="${AWS_REGION:-us-east-1}"
# Profile handling:
#   - If AWS_PROFILE is UNSET, default to 'globalpartners' (local dev)
#   - If AWS_PROFILE is SET BUT EMPTY (GitHub Actions sets it empty so the
#     CI step uses AWS_ACCESS_KEY_ID env vars), unset it so AWS CLI
#     doesn't try to look up a profile named "".
if [[ -z "${AWS_PROFILE+x}" ]]; then
  export AWS_PROFILE=globalpartners
elif [[ -z "$AWS_PROFILE" ]]; then
  unset AWS_PROFILE
fi
export AWS_PAGER=""  # never invoke a pager in scripts

# ------------------------------------------------------------
# Identity (looked up lazily the first time)
# ------------------------------------------------------------
if [[ -z "${ACCOUNT_ID:-}" ]]; then
  ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
fi
export ACCOUNT_ID

# ------------------------------------------------------------
# Resource names (deterministic -- re-runnable)
# ------------------------------------------------------------
export BUCKET_RAW="wistia-analytics-raw-${ACCOUNT_ID}"
export BUCKET_PROC="wistia-analytics-processed-${ACCOUNT_ID}"
export BUCKET_SCRIPTS="wistia-analytics-scripts-${ACCOUNT_ID}"

export GLUE_ROLE_NAME="wistia-glue-job-role"
export REDSHIFT_ROLE_NAME="wistia-redshift-s3-role"

export WISTIA_SECRET_NAME="wistia/api-token"
export REDSHIFT_SECRET_NAME="wistia/redshift-admin"

export SNS_TOPIC_NAME="wistia-pipeline-alerts"

export REDSHIFT_NAMESPACE="wistia-analytics-ns"
export REDSHIFT_WORKGROUP="wistia-analytics-wg"
export REDSHIFT_DB="wistia"
export REDSHIFT_ADMIN_USER="wistia_admin"
export REDSHIFT_BASE_CAPACITY="${REDSHIFT_BASE_CAPACITY:-8}"

export GLUE_JOB_INGEST="wistia-ingest"
export GLUE_JOB_TRANSFORM="wistia-transform"
export GLUE_JOB_DQLOAD="wistia-dq-and-load"
export GLUE_WORKFLOW="wistia-daily-workflow"
export SCHEDULE_CRON="${SCHEDULE_CRON:-cron(0 6 * * ? *)}"

# Two media IDs to track (from the requirement doc)
export MEDIA_IDS="${MEDIA_IDS:-gskhw4w4lm,v08dlrgr7v}"

# Required: set ALERT_EMAIL before running setup.sh so SNS knows where
# to send failure alerts. Intentionally no default -- we do not publish
# anyone's personal email in a public repo.
if [[ -z "${ALERT_EMAIL:-}" ]]; then
  # If a .env.local file exists (gitignored), source it for local dev
  if [[ -f "$SCRIPT_DIR/../.env.local" ]]; then
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/../.env.local"
  fi
fi
export ALERT_EMAIL="${ALERT_EMAIL:-}"

# Path helpers
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SCRIPT_DIR
export REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export IAM_DIR="$SCRIPT_DIR/iam"

# Convert an MSYS/Git-Bash path to a Windows-native path when running
# on Windows, so the Windows AWS CLI can read it. On Linux/macOS this
# is a no-op (cygpath won't exist -- just echo the input).
winpath() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$1"
  else
    echo "$1"
  fi
}

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
log()  { printf '\e[36m[%s]\e[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '  \e[32m\xE2\x9C\x93\e[0m %s\n' "$*"; }
skip() { printf '  \e[90m\xE2\x9A\xAC\e[0m %s\n' "$*"; }
warn() { printf '  \e[33m!\e[0m %s\n' "$*" >&2; }
die()  { printf '\e[31m[FATAL]\e[0m %s\n' "$*" >&2; exit 1; }

# ------------------------------------------------------------
# JSON-policy helpers (substitute placeholders in iam/*.json)
# ------------------------------------------------------------
render_policy() {
  local file="$1"
  sed \
    -e "s|__BUCKET_RAW__|${BUCKET_RAW}|g" \
    -e "s|__BUCKET_PROC__|${BUCKET_PROC}|g" \
    -e "s|__BUCKET_SCRIPTS__|${BUCKET_SCRIPTS}|g" \
    -e "s|__REGION__|${AWS_REGION}|g" \
    -e "s|__ACCOUNT__|${ACCOUNT_ID}|g" \
    "$file"
}

# ------------------------------------------------------------
# Idempotent "ensure" helpers.
# Each returns 0 on success; logs create/skip so re-runs are safe.
# ------------------------------------------------------------

# S3 bucket with versioning, encryption, public-access-block.
ensure_bucket() {
  local bucket="$1"
  if aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    skip "S3 bucket $bucket already exists"
  else
    log "Creating S3 bucket $bucket"
    # us-east-1 doesn't accept LocationConstraint
    if [[ "$AWS_REGION" == "us-east-1" ]]; then
      aws s3api create-bucket --bucket "$bucket" --region "$AWS_REGION" >/dev/null
    else
      aws s3api create-bucket --bucket "$bucket" --region "$AWS_REGION" \
        --create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
    fi
    ok "Bucket created"
  fi
  # Always enforce configuration (safe on re-apply)
  aws s3api put-bucket-versioning --bucket "$bucket" \
    --versioning-configuration Status=Enabled >/dev/null
  aws s3api put-bucket-encryption --bucket "$bucket" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' >/dev/null
  aws s3api put-public-access-block --bucket "$bucket" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" >/dev/null
}

# IAM role (trust policy only; permissions attached separately).
ensure_role() {
  local role_name="$1"
  local trust_file="$2"
  if aws iam get-role --role-name "$role_name" >/dev/null 2>&1; then
    skip "IAM role $role_name already exists"
  else
    log "Creating IAM role $role_name"
    aws iam create-role \
      --role-name "$role_name" \
      --assume-role-policy-document "$(cat "$trust_file")" \
      --tags Key=Project,Value=wistia-video-analytics Key=ManagedBy,Value=aws-cli-scripts \
      >/dev/null
    ok "Role created"
  fi
}

# Attach AWS-managed policy by ARN (idempotent).
attach_managed_policy() {
  local role_name="$1"
  local policy_arn="$2"
  aws iam attach-role-policy --role-name "$role_name" --policy-arn "$policy_arn" >/dev/null
}

# Put an inline policy on a role (overwrites -- safe on re-apply).
put_inline_policy() {
  local role_name="$1"
  local policy_name="$2"
  local policy_file="$3"
  # Pass the policy as a string so we avoid file:// path issues on Windows.
  aws iam put-role-policy \
    --role-name "$role_name" \
    --policy-name "$policy_name" \
    --policy-document "$(render_policy "$policy_file")" >/dev/null
}

# Secrets Manager -- create if missing; do NOT rotate existing values.
ensure_secret_if_missing() {
  local name="$1"
  local json_value="$2"
  if aws secretsmanager describe-secret --secret-id "$name" >/dev/null 2>&1; then
    skip "Secret $name already exists (not rotating)"
  else
    log "Creating secret $name"
    aws secretsmanager create-secret \
      --name "$name" \
      --secret-string "$json_value" >/dev/null
    ok "Secret created"
  fi
}

# Returns the ARN of a secret (must exist).
secret_arn() {
  aws secretsmanager describe-secret --secret-id "$1" --query ARN --output text
}

# SSM String parameter -- upsert (overwrite=true) with default value only if missing.
ensure_ssm_param() {
  local name="$1"
  local description="$2"
  local default_value="$3"
  if aws ssm get-parameter --name "$name" >/dev/null 2>&1; then
    skip "SSM parameter $name already exists"
  else
    log "Creating SSM parameter $name"
    aws ssm put-parameter --name "$name" --description "$description" \
      --type String --value "$default_value" >/dev/null
    ok "Parameter created"
  fi
}

# SNS topic -- create-topic is idempotent (returns existing ARN).
ensure_sns_topic() {
  local name="$1"
  aws sns create-topic --name "$name" --query TopicArn --output text
}

# Email subscription to an SNS topic (idempotent -- list first).
ensure_sns_email_subscription() {
  local topic_arn="$1"
  local email="$2"
  local existing
  existing=$(aws sns list-subscriptions-by-topic --topic-arn "$topic_arn" \
    --query "Subscriptions[?Endpoint=='$email'].SubscriptionArn | [0]" --output text)
  if [[ -n "$existing" && "$existing" != "None" ]]; then
    skip "Email $email already subscribed to topic"
  else
    log "Subscribing $email to topic"
    aws sns subscribe --topic-arn "$topic_arn" --protocol email \
      --notification-endpoint "$email" >/dev/null
    ok "Subscription created (confirm via email)"
  fi
}

# Glue job -- create if missing, update if present (so Python Shell
# / PySpark job definitions stay in sync with the script content).
upsert_glue_job() {
  local name="$1"
  local args_json="$2"   # JSON file on disk with the full create-job payload
  if aws glue get-job --job-name "$name" >/dev/null 2>&1; then
    log "Updating Glue job $name"
    # Convert {Name: ..., rest...} into {JobName, JobUpdate: rest} for update-job.
    # Pass a Windows path because python here may be native Windows Python.
    local update_payload
    update_payload=$(python - "$(winpath "$args_json")" "$name" <<'PYEOF'
import json, sys
src, name = sys.argv[1:3]
create = json.load(open(src))
create.pop("Name", None)
print(json.dumps({"JobName": name, "JobUpdate": create}))
PYEOF
    )
    aws glue update-job --cli-input-json "$update_payload" >/dev/null
    ok "Job updated"
  else
    log "Creating Glue job $name"
    aws glue create-job --cli-input-json "$(cat "$args_json")" >/dev/null
    ok "Job created"
  fi
}

# Glue workflow -- create if missing.
ensure_glue_workflow() {
  local name="$1"
  local description="$2"
  if aws glue get-workflow --name "$name" >/dev/null 2>&1; then
    skip "Glue workflow $name already exists"
  else
    log "Creating Glue workflow $name"
    aws glue create-workflow --name "$name" --description "$description" >/dev/null
    ok "Workflow created"
  fi
}

# Glue trigger -- create from a JSON payload. Idempotent.
ensure_glue_trigger() {
  local name="$1"
  local payload_file="$2"
  if aws glue get-trigger --name "$name" >/dev/null 2>&1; then
    skip "Glue trigger $name already exists"
  else
    log "Creating Glue trigger $name"
    aws glue create-trigger --cli-input-json "$(cat "$payload_file")" >/dev/null
    ok "Trigger created"
  fi
}
