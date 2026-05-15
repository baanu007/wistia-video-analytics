#!/usr/bin/env bash
# ============================================================
# deploy-code.sh -- Fast-path code deployment.
#
# What it does:
#   - Uploads the 3 Glue Python scripts from local to S3
#   - Triggers aws glue update-job to pick up script changes
#     (Glue reads the script each run, but update-job also
#      refreshes any default-argument changes)
#
# Use this for every code change. For infrastructure changes,
# use setup.sh instead.
# ============================================================

# If SCRIPTS_BUCKET is provided via env (e.g. from a GitHub Secret in CI),
# use it directly so common.sh does not have to call STS, which keeps the
# AWS account ID out of CI logs.
if [[ -n "${SCRIPTS_BUCKET:-}" ]]; then
  export BUCKET_SCRIPTS="${SCRIPTS_BUCKET}"
fi

# shellcheck source=common.sh
source "$(dirname "$0")/common.sh"

# Sanity check: bucket must be set somehow before we proceed.
: "${BUCKET_SCRIPTS:?BUCKET_SCRIPTS (or SCRIPTS_BUCKET) must be set before deploy}"

# Do not log the full bucket name (it contains the account ID) -- log a
# redacted form instead. Local devs still see the real bucket in their
# own shell because common.sh exports it.
log "Uploading Glue scripts to s3://<scripts-bucket>/glue_jobs/"

aws s3 cp "$(winpath "$REPO_ROOT/glue_jobs/ingest/wistia_ingest.py")" \
          "s3://$BUCKET_SCRIPTS/glue_jobs/ingest/wistia_ingest.py" >/dev/null
ok "wistia_ingest.py uploaded"

aws s3 cp "$(winpath "$REPO_ROOT/glue_jobs/transform/bronze_to_silver.py")" \
          "s3://$BUCKET_SCRIPTS/glue_jobs/transform/bronze_to_silver.py" >/dev/null
ok "bronze_to_silver.py uploaded"

aws s3 cp "$(winpath "$REPO_ROOT/glue_jobs/dq_load/dq_and_load.py")" \
          "s3://$BUCKET_SCRIPTS/glue_jobs/dq_load/dq_and_load.py" >/dev/null
ok "dq_and_load.py uploaded"

log "DONE. Glue jobs will pick up the new scripts on next run."
echo
echo "To trigger a run now:"
echo "  aws glue start-workflow-run --name $GLUE_WORKFLOW --region $AWS_REGION"
