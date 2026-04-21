-- =========================================================================
-- Reference copy-templates used by the DQ+Load Glue job
-- (dq_and_load.py builds these dynamically, but they're kept here for reference
--  and for manual debugging runs via the Redshift Query Editor.)
--
-- Replace:
--   {S3_SILVER_URI}   e.g. s3://wistia-analytics-processed-041282018868/dim_media/year=2026/month=04/day=20/
--   {IAM_ROLE_ARN}    e.g. arn:aws:iam::041282018868:role/wistia-redshift-s3-role
-- =========================================================================

-- ---- dim_media ----
BEGIN;
TRUNCATE public.stg_dim_media;

COPY public.stg_dim_media
FROM '{S3_SILVER_URI_DIM_MEDIA}'
IAM_ROLE '{IAM_ROLE_ARN}'
FORMAT AS PARQUET;

DELETE FROM public.dim_media
 USING public.stg_dim_media
 WHERE dim_media.media_id = stg_dim_media.media_id;

INSERT INTO public.dim_media
SELECT * FROM public.stg_dim_media;
COMMIT;


-- ---- dim_visitor ----
BEGIN;
TRUNCATE public.stg_dim_visitor;

COPY public.stg_dim_visitor
FROM '{S3_SILVER_URI_DIM_VISITOR}'
IAM_ROLE '{IAM_ROLE_ARN}'
FORMAT AS PARQUET;

DELETE FROM public.dim_visitor
 USING public.stg_dim_visitor
 WHERE dim_visitor.visitor_key = stg_dim_visitor.visitor_key;

INSERT INTO public.dim_visitor
SELECT * FROM public.stg_dim_visitor;
COMMIT;


-- ---- fact_media_engagement ----
BEGIN;
TRUNCATE public.stg_fact_media_engagement;

COPY public.stg_fact_media_engagement
FROM '{S3_SILVER_URI_FACT}'
IAM_ROLE '{IAM_ROLE_ARN}'
FORMAT AS PARQUET;

DELETE FROM public.fact_media_engagement
 USING public.stg_fact_media_engagement
 WHERE fact_media_engagement.event_key = stg_fact_media_engagement.event_key;

INSERT INTO public.fact_media_engagement
SELECT * FROM public.stg_fact_media_engagement;
COMMIT;
