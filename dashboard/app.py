"""
Wistia Video Analytics — local Streamlit dashboard.

Pulls live aggregates from Amazon Redshift Serverless via the redshift-data
API. No public Redshift endpoint required; auth uses the local AWS profile +
the wistia/redshift-admin secret in Secrets Manager (same path the Glue jobs
use).

Run:
    pip install -r requirements.txt
    streamlit run app.py
"""

import os
import time

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================
# CONFIG
# ============================================================
AWS_PROFILE = os.environ.get("AWS_PROFILE", "globalpartners")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
REDSHIFT_WORKGROUP = "wistia-analytics-wg"
REDSHIFT_DATABASE = "wistia"
REDSHIFT_SECRET_NAME = "wistia/redshift-admin"
QUERY_CACHE_TTL_SEC = 300  # five minutes


# ============================================================
# CLIENTS + QUERY HELPER (cached at process level)
# ============================================================
@st.cache_resource
def get_session():
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


@st.cache_resource
def get_secret_arn() -> str:
    sm = get_session().client("secretsmanager")
    return sm.describe_secret(SecretId=REDSHIFT_SECRET_NAME)["ARN"]


@st.cache_data(ttl=QUERY_CACHE_TTL_SEC, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    rsd = get_session().client("redshift-data")
    secret_arn = get_secret_arn()

    resp = rsd.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        SecretArn=secret_arn,
        Sql=sql,
    )
    stmt_id = resp["Id"]

    # Poll
    while True:
        desc = rsd.describe_statement(Id=stmt_id)
        if desc["Status"] == "FINISHED":
            break
        if desc["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Query failed: {desc.get('Error')}\nSQL:\n{sql}")
        time.sleep(0.4)

    if not desc.get("HasResultSet"):
        return pd.DataFrame()

    result = rsd.get_statement_result(Id=stmt_id)
    cols = [c["name"] for c in result["ColumnMetadata"]]
    rows = []
    for record in result["Records"]:
        row = []
        for cell in record:
            if "stringValue" in cell:
                row.append(cell["stringValue"])
            elif "longValue" in cell:
                row.append(cell["longValue"])
            elif "doubleValue" in cell:
                row.append(cell["doubleValue"])
            elif "booleanValue" in cell:
                row.append(cell["booleanValue"])
            else:
                row.append(None)
        rows.append(row)
    return pd.DataFrame(rows, columns=cols)


# ============================================================
# PAGE SETUP
# ============================================================
st.set_page_config(
    page_title="Wistia Video Analytics",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
    [data-testid="stMetricValue"] {font-size: 28px;}
    h1 {margin-bottom: 0;}
    .small-caption {color: #666; font-size: 13px;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# HEADER
# ============================================================
col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.title("Wistia Video Analytics")
    st.markdown(
        '<div class="small-caption">Live data from Amazon Redshift Serverless · '
        "pipeline auto-refreshes nightly at 10:00 PM Central</div>",
        unsafe_allow_html=True,
    )
with col_refresh:
    st.write("")
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()


# ============================================================
# 1. KPI STRIP
# ============================================================
@st.cache_data(ttl=QUERY_CACHE_TTL_SEC, show_spinner=False)
def fetch_kpis():
    media = run_query(
        """
        SELECT SUM(total_plays) AS total_plays,
               SUM(total_loads) AS total_loads,
               ROUND(SUM(hours_watched), 1) AS total_hours
          FROM wistia.public.dim_media
        """
    )
    fact = run_query(
        """
        SELECT COUNT(*)                        AS event_count,
               COUNT(DISTINCT visitor_key)     AS unique_visitors,
               ROUND(AVG(percent_viewed)*100, 1) AS avg_view_pct
          FROM wistia.public.fact_media_engagement
        """
    )
    return media.iloc[0], fact.iloc[0]


with st.spinner("Loading KPIs…"):
    media_row, fact_row = fetch_kpis()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Plays", f"{int(media_row.total_plays):,}")
k2.metric("Total Hours Watched", f"{float(media_row.total_hours):,}")
k3.metric("Unique Visitors", f"{int(fact_row.unique_visitors):,}")
k4.metric("Avg. % Viewed", f"{float(fact_row.avg_view_pct)}%")

st.divider()


# ============================================================
# 2. CHANNEL COMPARISON  (Facebook vs YouTube)
# ============================================================
st.subheader("Facebook vs YouTube")

@st.cache_data(ttl=QUERY_CACHE_TTL_SEC, show_spinner=False)
def fetch_channel():
    return run_query(
        """
        SELECT m.channel,
               COUNT(*)                          AS events,
               COUNT(DISTINCT f.visitor_key)     AS unique_visitors,
               ROUND(AVG(f.percent_viewed)*100,2) AS avg_view_pct
          FROM wistia.public.fact_media_engagement f
          JOIN wistia.public.dim_media m USING (media_id)
         GROUP BY m.channel
         ORDER BY events DESC
        """
    )


channel_df = fetch_channel()

c1, c2, c3 = st.columns(3)
with c1:
    fig = px.bar(
        channel_df, x="channel", y="events",
        color="channel",
        color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"},
        title="Total Events",
    )
    fig.update_layout(showlegend=False, height=320, margin=dict(t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    fig = px.bar(
        channel_df, x="channel", y="unique_visitors",
        color="channel",
        color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"},
        title="Unique Visitors",
    )
    fig.update_layout(showlegend=False, height=320, margin=dict(t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

with c3:
    fig = px.bar(
        channel_df, x="channel", y="avg_view_pct",
        color="channel",
        color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"},
        title="Avg. % Viewed",
    )
    fig.update_layout(showlegend=False, height=320, margin=dict(t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

st.dataframe(
    channel_df.rename(columns={
        "channel": "Channel",
        "events": "Events",
        "unique_visitors": "Unique Visitors",
        "avg_view_pct": "Avg % Viewed",
    }),
    use_container_width=True,
    hide_index=True,
)

st.divider()


# ============================================================
# 3. GEOGRAPHY
# ============================================================
st.subheader("Where viewers are watching from")

@st.cache_data(ttl=QUERY_CACHE_TTL_SEC, show_spinner=False)
def fetch_geo():
    return run_query(
        """
        SELECT country,
               COUNT(*)                       AS events,
               COUNT(DISTINCT visitor_key)    AS unique_visitors,
               ROUND(AVG(percent_viewed)*100,2) AS avg_view_pct
          FROM wistia.public.fact_media_engagement
         WHERE country IS NOT NULL
         GROUP BY country
         ORDER BY events DESC
         LIMIT 15
        """
    )


geo_df = fetch_geo()

g1, g2 = st.columns([3, 2])
with g1:
    fig = px.bar(
        geo_df.sort_values("events"),
        x="events", y="country",
        orientation="h",
        title="Top 15 countries by event count",
        color="events",
        color_continuous_scale="Blues",
    )
    fig.update_layout(height=480, margin=dict(t=40, b=10), coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with g2:
    st.markdown("**Top countries (detailed)**")
    st.dataframe(
        geo_df.rename(columns={
            "country": "Country",
            "events": "Events",
            "unique_visitors": "Unique Visitors",
            "avg_view_pct": "Avg % Viewed",
        }),
        use_container_width=True,
        hide_index=True,
        height=440,
    )

st.divider()


# ============================================================
# 4. DAILY TREND
# ============================================================
st.subheader("Daily engagement trend")

@st.cache_data(ttl=QUERY_CACHE_TTL_SEC, show_spinner=False)
def fetch_daily():
    return run_query(
        """
        SELECT event_date,
               m.channel,
               COUNT(*) AS events
          FROM wistia.public.fact_media_engagement f
          JOIN wistia.public.dim_media m USING (media_id)
         GROUP BY event_date, m.channel
         ORDER BY event_date
        """
    )


daily_df = fetch_daily()
if not daily_df.empty:
    daily_df["event_date"] = pd.to_datetime(daily_df["event_date"])
    fig = px.line(
        daily_df,
        x="event_date", y="events", color="channel",
        markers=True,
        color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"},
        title="Events per day, by channel",
    )
    fig.update_layout(height=380, margin=dict(t=40, b=10))
    fig.update_xaxes(title="")
    fig.update_yaxes(title="Events")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No daily-trend data yet.")

st.divider()


# ============================================================
# 5. VIEWING-DEPTH HISTOGRAM
# ============================================================
st.subheader("How deep into the video do viewers go?")

@st.cache_data(ttl=QUERY_CACHE_TTL_SEC, show_spinner=False)
def fetch_view_distribution():
    return run_query(
        """
        SELECT m.channel,
               (FLOOR(percent_viewed * 10) * 10)::int AS bucket_pct,
               COUNT(*) AS events
          FROM wistia.public.fact_media_engagement f
          JOIN wistia.public.dim_media m USING (media_id)
         GROUP BY m.channel, bucket_pct
         ORDER BY bucket_pct
        """
    )


view_df = fetch_view_distribution()
if not view_df.empty:
    fig = px.bar(
        view_df,
        x="bucket_pct", y="events", color="channel",
        barmode="group",
        color_discrete_map={"Facebook": "#1877F2", "YouTube": "#FF0000"},
        title="Distribution of viewing percentage (buckets of 10%)",
        labels={"bucket_pct": "% of video viewed", "events": "Events"},
    )
    fig.update_layout(height=380, margin=dict(t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No viewing-distribution data yet.")


# ============================================================
# FOOTER
# ============================================================
st.divider()
st.markdown(
    '<div class="small-caption">Data source: <code>wistia</code> Redshift Serverless workgroup '
    "(<code>wistia-analytics-wg</code>) · refreshed via Glue Workflow "
    "<code>wistia-daily-workflow</code> at 03:00 UTC daily · query cache TTL 5 min</div>",
    unsafe_allow_html=True,
)
