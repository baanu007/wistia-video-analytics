-- =========================================================================
-- Wistia Video Analytics - Redshift Serverless DDL
-- Database: wistia
-- Schema:   public (default)
--
-- Creates target star-schema tables and staging tables.
-- Staging tables are used by the DQ+Load Glue job for idempotent merges.
-- =========================================================================

-- -------------------------------------------------------------------------
-- dim_media  --  one row per video
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.dim_media (
    media_id          VARCHAR(64)    NOT NULL,
    name              VARCHAR(500),
    duration_seconds  DECIMAL(10,3),
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    status            VARCHAR(32),
    project_name      VARCHAR(200),
    section_name      VARCHAR(300),
    channel           VARCHAR(20),      -- YouTube | Facebook | Unknown (derived)
    total_plays       BIGINT,
    total_loads       BIGINT,
    play_rate         DECIMAL(10,6),
    hours_watched     DECIMAL(14,6),
    total_visitors    BIGINT,
    engagement        DECIMAL(10,6),
    load_date         DATE
)
DISTSTYLE ALL                  -- tiny table; replicate to every compute node
SORTKEY (media_id);

-- Staging table: no NOT NULL on media_id so COPY can load raw data incl.
-- occasional empty-object rows from Wistia. They get filtered by the
-- DELETE ... WHERE media_id IS NULL step before merging into the target.
CREATE TABLE IF NOT EXISTS public.stg_dim_media (
    media_id          VARCHAR(64),
    name              VARCHAR(500),
    duration_seconds  DECIMAL(10,3),
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    status            VARCHAR(32),
    project_name      VARCHAR(200),
    section_name      VARCHAR(300),
    channel           VARCHAR(20),
    total_plays       BIGINT,
    total_loads       BIGINT,
    play_rate         DECIMAL(10,6),
    hours_watched     DECIMAL(14,6),
    total_visitors    BIGINT,
    engagement        DECIMAL(10,6),
    load_date         DATE
);


-- -------------------------------------------------------------------------
-- dim_visitor  --  one row per visitor_key
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.dim_visitor (
    visitor_key       VARCHAR(256)   NOT NULL,
    first_seen_at     TIMESTAMP,
    last_active_at    TIMESTAMP,
    load_count        INTEGER,
    play_count        INTEGER,
    browser           VARCHAR(64),
    browser_version   VARCHAR(32),
    platform          VARCHAR(64),
    is_mobile         BOOLEAN,
    country           VARCHAR(128),     -- from Events, not Visitors endpoint
    region            VARCHAR(128),
    city              VARCHAR(128),
    last_ip           VARCHAR(64),
    load_date         DATE
)
DISTKEY (visitor_key)
SORTKEY (last_active_at, visitor_key);

CREATE TABLE IF NOT EXISTS public.stg_dim_visitor (
    visitor_key       VARCHAR(256),
    first_seen_at     TIMESTAMP,
    last_active_at    TIMESTAMP,
    load_count        INTEGER,
    play_count        INTEGER,
    browser           VARCHAR(64),
    browser_version   VARCHAR(32),
    platform          VARCHAR(64),
    is_mobile         BOOLEAN,
    country           VARCHAR(128),
    region            VARCHAR(128),
    city              VARCHAR(128),
    last_ip           VARCHAR(64),
    load_date         DATE
);


-- -------------------------------------------------------------------------
-- fact_media_engagement  --  one row per event_key
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.fact_media_engagement (
    event_key             VARCHAR(256)   NOT NULL,
    media_id              VARCHAR(64)    NOT NULL,
    visitor_key           VARCHAR(256),
    event_date            DATE,
    received_at           TIMESTAMP,
    ip                    VARCHAR(64),
    country               VARCHAR(128),
    region                VARCHAR(128),
    city                  VARCHAR(128),
    lat                   DECIMAL(10,6),
    lon                   DECIMAL(10,6),
    percent_viewed        DECIMAL(10,6),
    embed_url             VARCHAR(1000),
    media_name            VARCHAR(500),
    browser               VARCHAR(64),
    platform              VARCHAR(64),
    is_mobile             BOOLEAN,
    visitor_play_count    INTEGER,
    visitor_load_count    INTEGER,
    load_date             DATE
)
DISTKEY (media_id)             -- co-locate facts by media for dim_media joins
SORTKEY (event_date, media_id);

CREATE TABLE IF NOT EXISTS public.stg_fact_media_engagement (
    event_key             VARCHAR(256),
    media_id              VARCHAR(64),
    visitor_key           VARCHAR(256),
    event_date            DATE,
    received_at           TIMESTAMP,
    ip                    VARCHAR(64),
    country               VARCHAR(128),
    region                VARCHAR(128),
    city                  VARCHAR(128),
    lat                   DECIMAL(10,6),
    lon                   DECIMAL(10,6),
    percent_viewed        DECIMAL(10,6),
    embed_url             VARCHAR(1000),
    media_name            VARCHAR(500),
    browser               VARCHAR(64),
    platform              VARCHAR(64),
    is_mobile             BOOLEAN,
    visitor_play_count    INTEGER,
    visitor_load_count    INTEGER,
    load_date             DATE
);


-- -------------------------------------------------------------------------
-- Helpful views (optional)
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_engagement_daily AS
SELECT
    f.event_date,
    m.channel,
    m.media_id,
    m.name                AS media_name,
    COUNT(DISTINCT f.visitor_key) AS unique_visitors,
    COUNT(*)                       AS total_events,
    AVG(f.percent_viewed)          AS avg_percent_viewed
FROM public.fact_media_engagement f
JOIN public.dim_media m ON f.media_id = m.media_id
GROUP BY 1,2,3,4;

CREATE OR REPLACE VIEW public.v_top_countries AS
SELECT
    m.channel,
    f.country,
    COUNT(*) AS events,
    COUNT(DISTINCT f.visitor_key) AS unique_visitors
FROM public.fact_media_engagement f
JOIN public.dim_media m ON f.media_id = m.media_id
WHERE f.country IS NOT NULL
GROUP BY 1,2
ORDER BY events DESC;
