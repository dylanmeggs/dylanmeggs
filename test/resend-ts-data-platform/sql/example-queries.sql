-- Top spammy domains in last hour
SELECT
  domain,
  SUM(spam_reports) AS total_spam_reports
FROM domain_event_metrics
WHERE window_start >= dateadd(hour, -1, current_timestamp)
GROUP BY domain
ORDER BY total_spam_reports DESC;

-- Domains with high spam rate
SELECT
  domain,
  SUM(spam_reports)::float / NULLIF(SUM(event_count), 0) AS spam_rate
FROM domain_event_metrics
GROUP BY domain
HAVING SUM(event_count) > 100;
