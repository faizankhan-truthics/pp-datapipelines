WITH config AS (
    SELECT
        -- Minimum volume required to trigger an alert
        30 AS min_current_starts,
        100 AS min_baseline_starts,

        -- Minimum historical volume required for a segment
        -- to participate in the mix-adjusted overall baseline
        30 AS min_mix_baseline_starts,

        -- Business significance thresholds
        0.20 AS warning_relative_drop,
        0.30 AS critical_relative_drop,

        -- Statistical significance thresholds
        -2.00 AS warning_z_score,
        -3.00 AS critical_z_score,

        -- Minimum share of current traffic represented
        -- by segments with sufficient historical data
        0.90 AS min_mix_coverage
),

events AS (
    SELECT
        session_id,
        device_id,
        event_time,
        event_type,
        country,
        platform,

        JSON_VALUE(
            event_properties,
            '$.funnel_name'
        ) AS funnel_name,

        JSON_VALUE(
            event_properties,
            '$.screen_id'
        ) AS screen_id,

        JSON_VALUE(
            event_properties,
            '$.button_name'
        ) AS button_name,

        JSON_VALUE(
            event_properties,
            '$.payment_method'
        ) AS payment_method,

        JSON_VALUE(
            event_properties,
            '$.product_type'
        ) AS product_type

    FROM `{{ params.bq_project }}.{{ params.amplitude_stage_dataset }}.EVENTS_839561`

    WHERE event_time >= TIMESTAMP(
        DATE_SUB(
            CURRENT_DATE('UTC'),
            INTERVAL 16 DAY
        )
    )

      -- Exclude the current incomplete UTC day
      AND event_time < TIMESTAMP(
          CURRENT_DATE('UTC')
      )

      AND session_id IS NOT NULL
      AND session_id > 0
      AND device_id IS NOT NULL

      AND JSON_VALUE(
          event_properties,
          '$.funnel_name'
      ) IN (
          'phone_lookup',
          'image_lookup'
      )
),

session_steps AS (
    SELECT
        funnel_name,
        session_id,
        device_id,

        MIN(
            IF(
                event_type = 'first_launch',
                event_time,
                NULL
            )
        ) AS first_launch_time,

        MIN(
            IF(
                event_type = 'lookup_tap'
                AND button_name IS NULL,
                event_time,
                NULL
            )
        ) AS initial_lookup_time,

        MIN(
            IF(
                event_type = 'page_view'
                AND screen_id = 'sign_up',
                event_time,
                NULL
            )
        ) AS signup_page_time,

        MIN(
            IF(
                event_type = 'signup_complete',
                event_time,
                NULL
            )
        ) AS signup_complete_time,

        MIN(
            IF(
                event_type = 'page_view'
                AND screen_id = 'paywall',
                event_time,
                NULL
            )
        ) AS paywall_time,

        MIN(
            IF(
                event_type = 'purchase_started',
                event_time,
                NULL
            )
        ) AS purchase_started_time,

        MIN(
            IF(
                event_type = 'purchase'
                AND payment_method IS NOT NULL
                AND COALESCE(
                    product_type,
                    ''
                ) != 'upsell',
                event_time,
                NULL
            )
        ) AS purchase_time,

        ARRAY_AGG(
            IF(
                event_type = 'first_launch',
                STRUCT(
                    country,
                    platform
                ),
                NULL
            )
            IGNORE NULLS
            ORDER BY event_time
            LIMIT 1
        )[SAFE_OFFSET(0)] AS launch_details,

        ARRAY_AGG(
            IF(
                event_type = 'page_view'
                AND screen_id = 'sign_up',
                STRUCT(
                    country,
                    platform
                ),
                NULL
            )
            IGNORE NULLS
            ORDER BY event_time
            LIMIT 1
        )[SAFE_OFFSET(0)] AS signup_details,

        ARRAY_AGG(
            IF(
                event_type = 'page_view'
                AND screen_id = 'paywall',
                STRUCT(
                    country,
                    platform
                ),
                NULL
            )
            IGNORE NULLS
            ORDER BY event_time
            LIMIT 1
        )[SAFE_OFFSET(0)] AS paywall_details,

        ARRAY_AGG(
            IF(
                event_type = 'purchase_started',
                STRUCT(
                    country,
                    platform
                ),
                NULL
            )
            IGNORE NULLS
            ORDER BY event_time
            LIMIT 1
        )[SAFE_OFFSET(0)] AS payment_details

    FROM events

    GROUP BY
        funnel_name,
        session_id,
        device_id
),

metric_rows AS (

    -- 1. First Launch → Second Action
    SELECT
        DATE(first_launch_time) AS date,
        funnel_name,
        'First Launch → Second Action' AS metric,

        COALESCE(
            launch_details.country,
            'Unknown'
        ) AS country,

        COALESCE(
            launch_details.platform,
            'Unknown'
        ) AS platform,

        initial_lookup_time > first_launch_time
            AS completed

    FROM session_steps

    WHERE first_launch_time IS NOT NULL


    UNION ALL


    -- 2. First Screen → Purchase
    SELECT
        DATE(first_launch_time),
        funnel_name,
        'First Screen → Purchase',

        COALESCE(
            launch_details.country,
            'Unknown'
        ),

        COALESCE(
            launch_details.platform,
            'Unknown'
        ),

        purchase_time > first_launch_time

    FROM session_steps

    WHERE first_launch_time IS NOT NULL


    UNION ALL


    -- 3. Sign-Up Page → Successful Sign-Up
    SELECT
        DATE(signup_page_time),
        funnel_name,
        'Sign-Up Page → Successful Sign-Up',

        COALESCE(
            signup_details.country,
            'Unknown'
        ),

        COALESCE(
            signup_details.platform,
            'Unknown'
        ),

        signup_complete_time > signup_page_time

    FROM session_steps

    WHERE signup_page_time IS NOT NULL


    UNION ALL


    -- 4. Paywall → Purchase Started
    SELECT
        DATE(paywall_time),
        funnel_name,
        'Paywall → Purchase Started',

        COALESCE(
            paywall_details.country,
            'Unknown'
        ),

        COALESCE(
            paywall_details.platform,
            'Unknown'
        ),

        purchase_started_time > paywall_time

    FROM session_steps

    WHERE paywall_time IS NOT NULL


    UNION ALL


    -- 5. Payment Started → Payment Completed
    SELECT
        DATE(purchase_started_time),
        funnel_name,
        'Payment Started → Payment Completed',

        COALESCE(
            payment_details.country,
            'Unknown'
        ),

        COALESCE(
            payment_details.platform,
            'Unknown'
        ),

        purchase_time > purchase_started_time

    FROM session_steps

    WHERE purchase_started_time IS NOT NULL
),

segment_counts AS (
    SELECT
        funnel_name,
        metric,
        country,
        platform,

        COUNTIF(
            date = DATE_SUB(
                CURRENT_DATE('UTC'),
                INTERVAL 1 DAY
            )
        ) AS current_starts,

        COUNTIF(
            date = DATE_SUB(
                CURRENT_DATE('UTC'),
                INTERVAL 1 DAY
            )
            AND completed
        ) AS current_completions,

        COUNTIF(
            date BETWEEN
                DATE_SUB(
                    CURRENT_DATE('UTC'),
                    INTERVAL 15 DAY
                )
                AND
                DATE_SUB(
                    CURRENT_DATE('UTC'),
                    INTERVAL 2 DAY
                )
        ) AS baseline_starts,

        COUNTIF(
            date BETWEEN
                DATE_SUB(
                    CURRENT_DATE('UTC'),
                    INTERVAL 15 DAY
                )
                AND
                DATE_SUB(
                    CURRENT_DATE('UTC'),
                    INTERVAL 2 DAY
                )
            AND completed
        ) AS baseline_completions

    FROM metric_rows

    GROUP BY
        funnel_name,
        metric,
        country,
        platform
),

segment_rates AS (
    SELECT
        *,

        SAFE_DIVIDE(
            current_completions,
            current_starts
        ) AS current_rate,

        SAFE_DIVIDE(
            baseline_completions,
            baseline_starts
        ) AS baseline_rate

    FROM segment_counts
),

segment_stats AS (
    SELECT
        *,

        SAFE_DIVIDE(
            current_completions
                + baseline_completions,
            current_starts
                + baseline_starts
        ) AS pooled_rate

    FROM segment_rates
),

segment_final AS (
    SELECT
        DATE_SUB(
            CURRENT_DATE('UTC'),
            INTERVAL 1 DAY
        ) AS monitoring_date,

        funnel_name,
        metric,

        'Country × Platform'
            AS monitoring_level,

        country,
        platform,

        current_starts,
        current_completions,

        baseline_starts,
        baseline_completions,

        current_rate,
        baseline_rate,

        SAFE_DIVIDE(
            baseline_rate - current_rate,
            baseline_rate
        ) AS relative_drop,

        -- Two-proportion Z test
        SAFE_DIVIDE(
            current_rate - baseline_rate,

            SQRT(
                pooled_rate
                * (1 - pooled_rate)
                * (
                    SAFE_DIVIDE(
                        1,
                        current_starts
                    )
                    +
                    SAFE_DIVIDE(
                        1,
                        baseline_starts
                    )
                )
            )
        ) AS z_score,

        1.0 AS mix_coverage

    FROM segment_stats
),

overall_mix AS (
    SELECT
        s.funnel_name,
        s.metric,

        SUM(
            s.current_starts
        ) AS all_current_starts,

        -- Current traffic represented by usable
        -- historical Country × Platform segments
        SUM(
            IF(
                s.baseline_starts
                    >= cfg.min_mix_baseline_starts,

                s.current_starts,
                0
            )
        ) AS covered_current_starts,

        SUM(
            IF(
                s.baseline_starts
                    >= cfg.min_mix_baseline_starts,

                s.current_completions,
                0
            )
        ) AS covered_current_completions,

        -- Expected completions using today's traffic mix
        -- and each segment's historical conversion rate
        SUM(
            IF(
                s.baseline_starts
                    >= cfg.min_mix_baseline_starts,

                s.current_starts
                    * s.baseline_rate,

                0
            )
        ) AS expected_completions,

        -- Current-period variance under the historical rate
        SUM(
            IF(
                s.baseline_starts
                    >= cfg.min_mix_baseline_starts,

                s.current_starts
                    * s.baseline_rate
                    * (1 - s.baseline_rate),

                0
            )
        ) AS current_variance,

        -- Uncertainty in the estimated historical segment rates
        SUM(
            IF(
                s.baseline_starts
                    >= cfg.min_mix_baseline_starts
                AND s.baseline_starts > 0,

                POW(
                    s.current_starts,
                    2
                )
                * SAFE_DIVIDE(
                    s.baseline_rate
                        * (1 - s.baseline_rate),
                    s.baseline_starts
                ),

                0
            )
        ) AS baseline_estimation_variance,

        SUM(
            IF(
                s.baseline_starts
                    >= cfg.min_mix_baseline_starts,

                s.baseline_starts,
                0
            )
        ) AS covered_baseline_starts,

        SUM(
            IF(
                s.baseline_starts
                    >= cfg.min_mix_baseline_starts,

                s.baseline_completions,
                0
            )
        ) AS covered_baseline_completions

    FROM segment_rates s

    CROSS JOIN config cfg

    GROUP BY
        s.funnel_name,
        s.metric
),

overall_final AS (
    SELECT
        DATE_SUB(
            CURRENT_DATE('UTC'),
            INTERVAL 1 DAY
        ) AS monitoring_date,

        funnel_name,
        metric,

        'Overall - Mix Adjusted'
            AS monitoring_level,

        'ALL' AS country,
        'ALL' AS platform,

        covered_current_starts
            AS current_starts,

        covered_current_completions
            AS current_completions,

        covered_baseline_starts
            AS baseline_starts,

        covered_baseline_completions
            AS baseline_completions,

        SAFE_DIVIDE(
            covered_current_completions,
            covered_current_starts
        ) AS current_rate,

        SAFE_DIVIDE(
            expected_completions,
            covered_current_starts
        ) AS baseline_rate,

        SAFE_DIVIDE(
            SAFE_DIVIDE(
                expected_completions,
                covered_current_starts
            )
            -
            SAFE_DIVIDE(
                covered_current_completions,
                covered_current_starts
            ),

            SAFE_DIVIDE(
                expected_completions,
                covered_current_starts
            )
        ) AS relative_drop,

        -- Mix-adjusted statistical test including
        -- current + baseline estimation uncertainty
        SAFE_DIVIDE(
            covered_current_completions
                - expected_completions,

            SQRT(
                current_variance
                + baseline_estimation_variance
            )
        ) AS z_score,

        SAFE_DIVIDE(
            covered_current_starts,
            all_current_starts
        ) AS mix_coverage

    FROM overall_mix
),

all_results AS (

    SELECT
        monitoring_date,
        funnel_name,
        metric,
        monitoring_level,
        country,
        platform,
        current_starts,
        current_completions,
        baseline_starts,
        baseline_completions,
        current_rate,
        baseline_rate,
        relative_drop,
        z_score,
        mix_coverage

    FROM segment_final


    UNION ALL


    SELECT
        monitoring_date,
        funnel_name,
        metric,
        monitoring_level,
        country,
        platform,
        current_starts,
        current_completions,
        baseline_starts,
        baseline_completions,
        current_rate,
        baseline_rate,
        relative_drop,
        z_score,
        mix_coverage

    FROM overall_final
),

classified AS (
    SELECT
        r.*,

        CASE
            WHEN current_starts
                    >= cfg.min_current_starts

             AND baseline_starts
                    >= cfg.min_baseline_starts

             AND mix_coverage
                    >= cfg.min_mix_coverage

             AND relative_drop
                    >= cfg.critical_relative_drop

             AND z_score
                    <= cfg.critical_z_score

            THEN 'CRITICAL'


            WHEN current_starts
                    >= cfg.min_current_starts

             AND baseline_starts
                    >= cfg.min_baseline_starts

             AND mix_coverage
                    >= cfg.min_mix_coverage

             AND relative_drop
                    >= cfg.warning_relative_drop

             AND z_score
                    <= cfg.warning_z_score

            THEN 'WARNING'


            ELSE 'OK'
        END AS alert_status

    FROM all_results r

    CROSS JOIN config cfg
),

alerts AS (
    SELECT
        monitoring_date,
        funnel_name,
        metric,
        monitoring_level,
        country,
        platform,

        current_starts,
        current_completions,
        current_rate,

        baseline_starts,
        baseline_completions,
        baseline_rate,

        relative_drop,
        z_score,
        mix_coverage,
        alert_status

    FROM classified

    WHERE alert_status IN (
        'WARNING',
        'CRITICAL'
    )
),

-- Everything below renders the alert rows into the Slack message text itself,
-- so the DAG doesn't need any alert-specific formatting logic -- it just
-- takes the `message` column from the single output row and posts it. A
-- future alert only needs a SQL file shaped like this one (a `title` +
-- `message` column) to reuse the same generic Slack-alert DAG code.

alert_lines AS (
    SELECT
        monitoring_date,
        funnel_name,
        alert_status,
        monitoring_level,
        metric,

        CASE alert_status
            WHEN 'CRITICAL' THEN 1
            WHEN 'WARNING' THEN 2
        END AS severity_rank,

        FORMAT(
            '%s %s — %s\nCVR: %.2f%% vs %.2f%% | ↓ %.2f%% | Starts: %d%s',
            IF(alert_status = 'CRITICAL', '🔴', '🟠'),
            metric,
            IF(
                monitoring_level = 'Overall - Mix Adjusted',
                'Overall',
                CONCAT(country, ' / ', platform)
            ),
            ROUND(current_rate * 100, 2),
            ROUND(baseline_rate * 100, 2),
            ROUND(relative_drop * 100, 2),
            current_starts,
            IF(
                monitoring_level = 'Overall - Mix Adjusted',
                FORMAT(' | Mix coverage: %.2f%%', ROUND(mix_coverage * 100, 2)),
                ''
            )
        ) AS alert_line

    FROM alerts
),

-- One header + its alert lines per (date, funnel) group.
group_sections AS (
    SELECT
        monitoring_date,
        funnel_name,

        FORMAT(
            'Date: %s | Funnel: %s | Baseline: Previous 14 days\n%s',
            CAST(monitoring_date AS STRING),
            funnel_name,
            STRING_AGG(
                alert_line,
                '\n\n'
                ORDER BY severity_rank, monitoring_level, metric
            )
        ) AS section_text

    FROM alert_lines

    GROUP BY
        monitoring_date,
        funnel_name
),

severity_totals AS (
    SELECT
        COUNTIF(alert_status = 'CRITICAL') AS critical_count,
        COUNTIF(alert_status = 'WARNING') AS warning_count
    FROM alerts
)

SELECT
    FORMAT(
        '🚨 InfoChecker CVR Alerts — %s',
        ARRAY_TO_STRING(
            ARRAY(
                SELECT label
                FROM UNNEST([
                    IF(
                        critical_count > 0,
                        FORMAT('%d critical%s', critical_count, IF(critical_count = 1, '', 's')),
                        NULL
                    ),
                    IF(
                        warning_count > 0,
                        FORMAT('%d warning%s', warning_count, IF(warning_count = 1, '', 's')),
                        NULL
                    )
                ]) AS label
                WHERE label IS NOT NULL
            ),
            ', '
        )
    ) AS title,

    (
        SELECT STRING_AGG(section_text, '\n\n' ORDER BY monitoring_date, funnel_name)
        FROM group_sections
    ) AS message

FROM severity_totals

WHERE critical_count + warning_count > 0;