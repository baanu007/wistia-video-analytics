#!/usr/bin/env bash
# ============================================================
# create-tables.sh -- Run sql/create_tables.sql against the
# Redshift Serverless workgroup via the Redshift Data API.
#
# Idempotent: uses CREATE TABLE IF NOT EXISTS, so re-running
# this against an initialized database is a no-op.
#
# Auth: SecretArn (admin credentials). Tables are owned by the
# admin user, so the DQ+Load Glue job (also running as admin)
# has full access.
# ============================================================

# shellcheck source=common.sh
source "$(dirname "$0")/common.sh"

SECRET_ARN=$(secret_arn "$REDSHIFT_SECRET_NAME")
log "Running sql/create_tables.sql on $REDSHIFT_WORKGROUP"

python - "$SECRET_ARN" "$REDSHIFT_WORKGROUP" "$REDSHIFT_DB" "$(winpath "$REPO_ROOT/sql/create_tables.sql")" <<'PYEOF'
import boto3, sys, time, pathlib
secret_arn, workgroup, db, sql_file = sys.argv[1:5]
c = boto3.client("redshift-data")
sql = pathlib.Path(sql_file).read_text()
resp = c.execute_statement(WorkgroupName=workgroup, Database=db, SecretArn=secret_arn, Sql=sql)
sid = resp["Id"]
while True:
    d = c.describe_statement(Id=sid)
    if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
        break
    time.sleep(2)
print(f"Status: {d['Status']}")
if d["Status"] != "FINISHED":
    print(f"Error: {d.get('Error')}")
    sys.exit(1)
PYEOF

ok "Redshift tables created / verified"
