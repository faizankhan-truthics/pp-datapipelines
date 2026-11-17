-- include/sql/posthog/alerts/data_checks_alert.sql
-- PostHog export health, rendered as one Slack alert (title, message), or no row when all
-- is well. The export runs inside PostHog, so Airflow never sees it fail: this is what
-- catches it. Per configured project:
--   * freshness: newest bronze.posthog_events row loaded more than late_hours ago, or none
--     in the window; persons/sessions (no load-time column) by table last-modified time;
--   * volume drop: yesterday's events below volume_drop_ratio x the previous 7-day average;
--   * duplicates: share of duplicate uuids yesterday -- information only, added to the
--     message when something else alerts.
-- Reads only the last lookback_days partitions of the events table (partitioned by
-- DATE(timestamp)) and the dataset's table metadata, so it stays cheap.

WITH projects AS (
  SELECT team_id
  FROM UNNEST([{{ params.team_ids | join(', ') }}]) AS team_id
),

recent_events AS (
  SELECT team_id, uuid, DATE(timestamp) AS event_date, bq_ingested_timestamp
  FROM `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.{{ params.events_table }}`
  WHERE timestamp >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {{ params.lookback_days }} DAY))
),

per_project AS (
  SELECT
    p.team_id,
    MAX(e.bq_ingested_timestamp) AS last_ingested_at,
    COUNTIF(e.event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS rows_yesterday,
    COUNT(DISTINCT IF(e.event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), e.uuid, NULL)) AS events_yesterday,
    COUNT(DISTINCT IF(
      e.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY),
      e.uuid, NULL
    )) / 7 AS avg_events_prev_7d
  FROM projects p
  LEFT JOIN recent_events e USING (team_id)
  GROUP BY p.team_id
),

table_freshness AS (
  SELECT
    expected_table_id AS table_id,
    TIMESTAMP_MILLIS(t.last_modified_time) AS last_modified_at
  FROM UNNEST(['{{ params.persons_table }}', '{{ params.sessions_table }}']) AS expected_table_id
  LEFT JOIN `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.__TABLES__` t
    ON t.table_id = expected_table_id
),

problems AS (
  SELECT FORMAT('project %d: no events exported in the last %d days', team_id, {{ params.lookback_days }}) AS line
  FROM per_project
  WHERE last_ingested_at IS NULL

  UNION ALL

  SELECT FORMAT(
    'project %d: newest event loaded %s, %d h ago (limit %s h)',
    team_id, FORMAT_TIMESTAMP('%F %R UTC', last_ingested_at),
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_ingested_at, HOUR), '{{ params.late_hours }}'
  )
  FROM per_project
  WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_ingested_at, MINUTE) > {{ params.late_hours }} * 60

  UNION ALL

  SELECT FORMAT(
    'project %d: %d events yesterday vs %d/day over the previous 7 days (alert below %s)',
    team_id, events_yesterday, CAST(avg_events_prev_7d AS INT64), '{{ params.volume_drop_ratio }}'
  )
  FROM per_project
  WHERE avg_events_prev_7d > 0
    AND events_yesterday < {{ params.volume_drop_ratio }} * avg_events_prev_7d

  UNION ALL

  SELECT IF(
    last_modified_at IS NULL,
    FORMAT('%s: table not found', table_id),
    FORMAT('%s: last modified %s, %d h ago (limit %s h)',
      table_id, FORMAT_TIMESTAMP('%F %R UTC', last_modified_at),
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_at, HOUR), '{{ params.late_hours }}')
  )
  FROM table_freshness
  WHERE last_modified_at IS NULL
     OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_at, MINUTE) > {{ params.late_hours }} * 60
),

duplicates AS (
  SELECT STRING_AGG(
    FORMAT('project %d: %.3f%% duplicate uuids yesterday (%d rows, %d events)',
      team_id, 100 * SAFE_DIVIDE(rows_yesterday - events_yesterday, rows_yesterday),
      rows_yesterday, events_yesterday),
    '\n' ORDER BY team_id
  ) AS lines
  FROM per_project
  WHERE rows_yesterday > 0
)

SELECT
  'PostHog export: data check failed ({{ params.env }})' AS title,
  CONCAT(
    STRING_AGG(p.line, '\n' ORDER BY p.line),
    IFNULL(CONCAT('\n\nFor information:\n', ANY_VALUE(d.lines)), ''),
    '\n\nThe export is managed in PostHog: Data management -> Destinations.'
  ) AS message
FROM problems p
CROSS JOIN duplicates d
HAVING COUNT(*) > 0
