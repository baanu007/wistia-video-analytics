# Wistia Video Analytics — Streamlit Dashboard

A local Streamlit app that connects to the live Redshift Serverless warehouse
and renders five sections of insight:

1. **KPI strip** — total plays, total hours watched, unique visitors, average % viewed
2. **Facebook vs YouTube** — events, unique visitors, average view depth side-by-side
3. **Geographic distribution** — top 15 countries by event count
4. **Daily engagement trend** — events per day, broken out by channel
5. **Viewing-depth histogram** — how far into the video viewers actually get

The dashboard reads from Redshift through the `redshift-data` API using your
local AWS profile. The Redshift workgroup stays private — no public endpoint,
no VPC peering, no firewall changes.

## Prerequisites

- Python 3.10+
- AWS CLI configured with a profile that can:
  - read the secret `wistia/redshift-admin` in Secrets Manager
  - call `redshift-data:ExecuteStatement` / `DescribeStatement` / `GetStatementResult` against the `wistia-analytics-wg` workgroup
- The Wistia pipeline must have run at least once (the dashboard queries assume the star schema is populated)

## Install

```bash
cd dashboard/
pip install -r requirements.txt
```

## Run

```bash
# default profile is "globalpartners" / region us-east-1
streamlit run app.py

# or override:
AWS_PROFILE=myprofile AWS_REGION=us-east-1 streamlit run app.py
```

Streamlit opens at <http://localhost:8501> by default.

## Notes

- Query results are cached for 5 minutes via `st.cache_data` — click **Refresh
  data** at the top right to clear the cache and re-query Redshift.
- All queries are aggregations (no row-level pulls), so the redshift-data
  API's result-size limit is comfortable.
- Auth uses the `wistia/redshift-admin` secret rather than IAM-derived users
  — same reason the Glue jobs do: the IAM-derived user has no table grants.
