CREATE TABLE IF NOT EXISTS `lead-insight-platform.lead_platform.stg_job_postings` (
  posting_id STRING NOT NULL,
  source STRING NOT NULL,
  original_url STRING NOT NULL,
  company_name STRING,
  title STRING,
  location STRING,
  employment_type STRING,
  experience_level STRING,
  description_text STRING,
  skills STRING,
  collected_at TIMESTAMP NOT NULL,
  content_hash STRING NOT NULL,
  raw_s3_key STRING NOT NULL,
  processed_s3_key STRING NOT NULL,
  curated_s3_key STRING NOT NULL
);