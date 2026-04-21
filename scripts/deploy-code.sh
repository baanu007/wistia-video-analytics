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

# shellcheck source=common.sh
source "$(dirname "$0")/common.sh"

log "Uploading Glue scripts to s3://$BUCKET_SCRIPTS/glue_jobs/"

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
