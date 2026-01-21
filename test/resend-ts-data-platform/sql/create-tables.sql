-- Source of truth event streaming data
CREATE EXTERNAL TABLE spectrum_raw_trust_safety_events (
  event_id        VARCHAR,
  user_id         VARCHAR,
  email           VARCHAR,
  event_type      VARCHAR,
  timestamp       TIMESTAMP,
  is_spam         BOOLEAN,
  metadata        SUPER
)
PARTITIONED BY (dt DATE)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://resend-ts-raw/events/';



CREATE TABLE domain_event_metrics (
  domain VARCHAR(255),
  window_start TIMESTAMP,
  event_count INT,
  spam_reports INT
)
DISTSTYLE AUTO
SORTKEY (window_start);


CREATE TABLE trust_safety_domain_metrics AS
SELECT
  dt,
  SPLIT_PART(email, '@', 2) AS email_domain,
  event_type,
  COUNT(*) AS event_count,
  SUM(CASE WHEN is_spam THEN 1 ELSE 0 END) AS spam_count
FROM spectrum_raw_trust_safety_events
GROUP BY 1,2,3;


INSERT INTO trust_safety_domain_metrics
SELECT
  dt,
  SPLIT_PART(email, '@', 2),
  event_type,
  COUNT(*),
  SUM(CASE WHEN is_spam THEN 1 ELSE 0 END)
FROM spectrum_raw_trust_safety_events
WHERE dt = CURRENT_DATE - 1
GROUP BY 1,2,3;

-- Spam score?
CASE
  WHEN spam_count / NULLIF(event_count, 0) > 0.4 THEN true
  WHEN event_count > 100 AND spam_count > 20 THEN true
  ELSE false
END AS domain_flagged
