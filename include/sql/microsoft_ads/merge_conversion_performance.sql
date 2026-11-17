-- Bronze upsert for Microsoft Ads ConversionPerformanceReportRequest.
-- Grain: account_id x campaign_id x ad_group_id x keyword_id x goal_id x time_period
-- -- one level beyond keyword_performance, since this report splits by conversion
-- goal too. time_period kept exactly as Microsoft returns it (shape depends on
-- aggregation_type). Re-pulled every run across a trailing lookback window to catch
-- retroactive corrections -- QUALIFY dedupes to the latest-seen row per key.
--
-- The ON clause uses `IS NOT DISTINCT FROM` for goal_id, defensively -- plain `=`
-- never matches NULL to NULL, which would re-insert a duplicate every run instead of
-- updating in place. Confirmed via real data (2026-11-12): Microsoft actually sends
-- the literal value "0" for "no goal," not NULL, so this is a safeguard, not a fix
-- for an observed problem.
--
-- Ctr/ConversionRate/AllConversionRate come back with a literal "%" suffix --
-- REPLACE(..., '%', '') strips it before SAFE_CAST, same fix as every endpoint since
-- ad_group_performance. No Ptr on this report.

CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.microsoft_ads_{{ params.endpoint }}` (
    account_id STRING NOT NULL,
    account_name STRING,
    account_number STRING,
    account_status STRING,
    campaign_id STRING NOT NULL,
    campaign_name STRING,
    campaign_status STRING,
    ad_group_id STRING NOT NULL,
    ad_group_name STRING,
    ad_group_status STRING,
    keyword STRING,
    keyword_id STRING NOT NULL,
    keyword_status STRING,
    goal STRING,
    goal_id STRING,
    goal_type STRING,
    marketing_objective STRING,
    channel STRING,
    sub_channel STRING,
    time_period STRING NOT NULL,
    aggregation_type STRING NOT NULL,
    impressions INT64,
    clicks INT64,
    ctr FLOAT64,
    spend FLOAT64,
    conversions_qualified FLOAT64,
    conversion_rate FLOAT64,
    cost_per_conversion FLOAT64,
    all_conversions_qualified FLOAT64,
    all_conversion_rate FLOAT64,
    all_cost_per_conversion FLOAT64,
    all_return_on_ad_spend FLOAT64,
    all_revenue FLOAT64,
    all_revenue_per_conversion FLOAT64,
    return_on_ad_spend FLOAT64,
    revenue FLOAT64,
    revenue_per_conversion FLOAT64,
    assists INT64,
    cost_per_assist FLOAT64,
    revenue_per_assist FLOAT64,
    view_through_conversions_qualified FLOAT64,
    view_through_revenue FLOAT64,
    _elt_account_id STRING NOT NULL,
    _elt_endpoint STRING NOT NULL,
    _elt_insert_timestamp TIMESTAMP NOT NULL,
    _elt_update_timestamp TIMESTAMP NOT NULL,
    _gcs_file_path STRING NOT NULL
);

-- Self-healing schema migration: keeps an already-existing bronze table in sync with any
-- column added above without a manual ALTER TABLE per environment.
ALTER TABLE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.microsoft_ads_{{ params.endpoint }}`
  ADD COLUMN IF NOT EXISTS account_name STRING,
  ADD COLUMN IF NOT EXISTS account_number STRING,
  ADD COLUMN IF NOT EXISTS account_status STRING,
  ADD COLUMN IF NOT EXISTS campaign_name STRING,
  ADD COLUMN IF NOT EXISTS campaign_status STRING,
  ADD COLUMN IF NOT EXISTS ad_group_name STRING,
  ADD COLUMN IF NOT EXISTS ad_group_status STRING,
  ADD COLUMN IF NOT EXISTS keyword STRING,
  ADD COLUMN IF NOT EXISTS keyword_status STRING,
  ADD COLUMN IF NOT EXISTS goal STRING,
  ADD COLUMN IF NOT EXISTS goal_id STRING,
  ADD COLUMN IF NOT EXISTS goal_type STRING,
  ADD COLUMN IF NOT EXISTS marketing_objective STRING,
  ADD COLUMN IF NOT EXISTS channel STRING,
  ADD COLUMN IF NOT EXISTS sub_channel STRING,
  ADD COLUMN IF NOT EXISTS time_period STRING,
  ADD COLUMN IF NOT EXISTS aggregation_type STRING,
  ADD COLUMN IF NOT EXISTS impressions INT64,
  ADD COLUMN IF NOT EXISTS clicks INT64,
  ADD COLUMN IF NOT EXISTS ctr FLOAT64,
  ADD COLUMN IF NOT EXISTS spend FLOAT64,
  ADD COLUMN IF NOT EXISTS conversions_qualified FLOAT64,
  ADD COLUMN IF NOT EXISTS conversion_rate FLOAT64,
  ADD COLUMN IF NOT EXISTS cost_per_conversion FLOAT64,
  ADD COLUMN IF NOT EXISTS all_conversions_qualified FLOAT64,
  ADD COLUMN IF NOT EXISTS all_conversion_rate FLOAT64,
  ADD COLUMN IF NOT EXISTS all_cost_per_conversion FLOAT64,
  ADD COLUMN IF NOT EXISTS all_return_on_ad_spend FLOAT64,
  ADD COLUMN IF NOT EXISTS all_revenue FLOAT64,
  ADD COLUMN IF NOT EXISTS all_revenue_per_conversion FLOAT64,
  ADD COLUMN IF NOT EXISTS return_on_ad_spend FLOAT64,
  ADD COLUMN IF NOT EXISTS revenue FLOAT64,
  ADD COLUMN IF NOT EXISTS revenue_per_conversion FLOAT64,
  ADD COLUMN IF NOT EXISTS assists INT64,
  ADD COLUMN IF NOT EXISTS cost_per_assist FLOAT64,
  ADD COLUMN IF NOT EXISTS revenue_per_assist FLOAT64,
  ADD COLUMN IF NOT EXISTS view_through_conversions_qualified FLOAT64,
  ADD COLUMN IF NOT EXISTS view_through_revenue FLOAT64;

MERGE `{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.microsoft_ads_{{ params.endpoint }}` T
USING (
  SELECT
    JSON_VALUE(jp, '$.AccountId')                                        AS account_id,
    JSON_VALUE(jp, '$.AccountName')                                      AS account_name,
    JSON_VALUE(jp, '$.AccountNumber')                                    AS account_number,
    JSON_VALUE(jp, '$.AccountStatus')                                    AS account_status,
    JSON_VALUE(jp, '$.CampaignId')                                       AS campaign_id,
    JSON_VALUE(jp, '$.CampaignName')                                     AS campaign_name,
    JSON_VALUE(jp, '$.CampaignStatus')                                   AS campaign_status,
    JSON_VALUE(jp, '$.AdGroupId')                                        AS ad_group_id,
    JSON_VALUE(jp, '$.AdGroupName')                                      AS ad_group_name,
    JSON_VALUE(jp, '$.AdGroupStatus')                                    AS ad_group_status,
    JSON_VALUE(jp, '$.Keyword')                                          AS keyword,
    JSON_VALUE(jp, '$.KeywordId')                                        AS keyword_id,
    JSON_VALUE(jp, '$.KeywordStatus')                                    AS keyword_status,
    JSON_VALUE(jp, '$.Goal')                                             AS goal,
    JSON_VALUE(jp, '$.GoalId')                                           AS goal_id,
    JSON_VALUE(jp, '$.GoalType')                                         AS goal_type,
    JSON_VALUE(jp, '$.MarketingObjective')                               AS marketing_objective,
    JSON_VALUE(jp, '$.Channel')                                          AS channel,
    JSON_VALUE(jp, '$.SubChannel')                                       AS sub_channel,
    JSON_VALUE(jp, '$.TimePeriod')                                       AS time_period,
    aggregation_type,
    SAFE_CAST(JSON_VALUE(jp, '$.Impressions') AS INT64)                  AS impressions,
    SAFE_CAST(JSON_VALUE(jp, '$.Clicks') AS INT64)                       AS clicks,
    SAFE_CAST(REPLACE(JSON_VALUE(jp, '$.Ctr'), '%', '') AS FLOAT64)       AS ctr,
    SAFE_CAST(JSON_VALUE(jp, '$.Spend') AS FLOAT64)                      AS spend,
    SAFE_CAST(JSON_VALUE(jp, '$.ConversionsQualified') AS FLOAT64)       AS conversions_qualified,
    SAFE_CAST(REPLACE(JSON_VALUE(jp, '$.ConversionRate'), '%', '') AS FLOAT64) AS conversion_rate,
    SAFE_CAST(JSON_VALUE(jp, '$.CostPerConversion') AS FLOAT64)          AS cost_per_conversion,
    SAFE_CAST(JSON_VALUE(jp, '$.AllConversionsQualified') AS FLOAT64)    AS all_conversions_qualified,
    SAFE_CAST(REPLACE(JSON_VALUE(jp, '$.AllConversionRate'), '%', '') AS FLOAT64) AS all_conversion_rate,
    SAFE_CAST(JSON_VALUE(jp, '$.AllCostPerConversion') AS FLOAT64)       AS all_cost_per_conversion,
    SAFE_CAST(JSON_VALUE(jp, '$.AllReturnOnAdSpend') AS FLOAT64)         AS all_return_on_ad_spend,
    SAFE_CAST(JSON_VALUE(jp, '$.AllRevenue') AS FLOAT64)                 AS all_revenue,
    SAFE_CAST(JSON_VALUE(jp, '$.AllRevenuePerConversion') AS FLOAT64)    AS all_revenue_per_conversion,
    SAFE_CAST(JSON_VALUE(jp, '$.ReturnOnAdSpend') AS FLOAT64)            AS return_on_ad_spend,
    SAFE_CAST(JSON_VALUE(jp, '$.Revenue') AS FLOAT64)                    AS revenue,
    SAFE_CAST(JSON_VALUE(jp, '$.RevenuePerConversion') AS FLOAT64)       AS revenue_per_conversion,
    SAFE_CAST(JSON_VALUE(jp, '$.Assists') AS INT64)                      AS assists,
    SAFE_CAST(JSON_VALUE(jp, '$.CostPerAssist') AS FLOAT64)              AS cost_per_assist,
    SAFE_CAST(JSON_VALUE(jp, '$.RevenuePerAssist') AS FLOAT64)           AS revenue_per_assist,
    SAFE_CAST(JSON_VALUE(jp, '$.ViewThroughConversionsQualified') AS FLOAT64) AS view_through_conversions_qualified,
    SAFE_CAST(JSON_VALUE(jp, '$.ViewThroughRevenue') AS FLOAT64)         AS view_through_revenue,
    _elt_account_id,
    _elt_endpoint,
    CAST(_elt_insert_timestamp AS TIMESTAMP)                             AS _elt_insert_timestamp,
    CAST(_elt_update_timestamp AS TIMESTAMP)                             AS _elt_update_timestamp,
    _gcs_file_path
  FROM (
    SELECT *, SAFE.PARSE_JSON(raw_data, wide_number_mode => 'round') AS jp
    FROM `{{ params.bq_project }}.{{ params.bq_staging_dataset }}.microsoft_ads_{{ params.endpoint }}`
  )
  WHERE JSON_VALUE(jp, '$.AccountId') IS NOT NULL
    AND JSON_VALUE(jp, '$.CampaignId') IS NOT NULL
    AND JSON_VALUE(jp, '$.AdGroupId') IS NOT NULL
    AND JSON_VALUE(jp, '$.KeywordId') IS NOT NULL
    AND JSON_VALUE(jp, '$.TimePeriod') IS NOT NULL
  -- goal_id deliberately NOT required NOT NULL -- see the ON clause's goal_id note above.
  -- Keep only the latest-seen state per account/campaign/ad group/keyword/goal/time_period
  -- within this batch.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      JSON_VALUE(jp, '$.AccountId'), JSON_VALUE(jp, '$.CampaignId'),
      JSON_VALUE(jp, '$.AdGroupId'), JSON_VALUE(jp, '$.KeywordId'),
      JSON_VALUE(jp, '$.GoalId'), JSON_VALUE(jp, '$.TimePeriod')
    ORDER BY _elt_insert_timestamp DESC
  ) = 1
) S
ON T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.ad_group_id = S.ad_group_id
  AND T.keyword_id = S.keyword_id
  AND T.goal_id IS NOT DISTINCT FROM S.goal_id
  AND T.time_period = S.time_period
WHEN MATCHED THEN
    UPDATE SET
        T.account_name = S.account_name,
        T.account_number = S.account_number,
        T.account_status = S.account_status,
        T.campaign_name = S.campaign_name,
        T.campaign_status = S.campaign_status,
        T.ad_group_name = S.ad_group_name,
        T.ad_group_status = S.ad_group_status,
        T.keyword = S.keyword,
        T.keyword_status = S.keyword_status,
        T.goal = S.goal,
        T.goal_type = S.goal_type,
        T.marketing_objective = S.marketing_objective,
        T.channel = S.channel,
        T.sub_channel = S.sub_channel,
        T.aggregation_type = S.aggregation_type,
        T.impressions = S.impressions,
        T.clicks = S.clicks,
        T.ctr = S.ctr,
        T.spend = S.spend,
        T.conversions_qualified = S.conversions_qualified,
        T.conversion_rate = S.conversion_rate,
        T.cost_per_conversion = S.cost_per_conversion,
        T.all_conversions_qualified = S.all_conversions_qualified,
        T.all_conversion_rate = S.all_conversion_rate,
        T.all_cost_per_conversion = S.all_cost_per_conversion,
        T.all_return_on_ad_spend = S.all_return_on_ad_spend,
        T.all_revenue = S.all_revenue,
        T.all_revenue_per_conversion = S.all_revenue_per_conversion,
        T.return_on_ad_spend = S.return_on_ad_spend,
        T.revenue = S.revenue,
        T.revenue_per_conversion = S.revenue_per_conversion,
        T.assists = S.assists,
        T.cost_per_assist = S.cost_per_assist,
        T.revenue_per_assist = S.revenue_per_assist,
        T.view_through_conversions_qualified = S.view_through_conversions_qualified,
        T.view_through_revenue = S.view_through_revenue,
        T._elt_update_timestamp = CURRENT_TIMESTAMP(),
        T._gcs_file_path = S._gcs_file_path
WHEN NOT MATCHED THEN
    INSERT (
        account_id, account_name, account_number, account_status,
        campaign_id, campaign_name, campaign_status,
        ad_group_id, ad_group_name, ad_group_status,
        keyword, keyword_id, keyword_status,
        goal, goal_id, goal_type,
        marketing_objective, channel, sub_channel,
        time_period, aggregation_type,
        impressions, clicks, ctr, spend,
        conversions_qualified, conversion_rate, cost_per_conversion,
        all_conversions_qualified, all_conversion_rate, all_cost_per_conversion,
        all_return_on_ad_spend, all_revenue, all_revenue_per_conversion,
        return_on_ad_spend, revenue, revenue_per_conversion,
        assists, cost_per_assist, revenue_per_assist,
        view_through_conversions_qualified, view_through_revenue,
        _elt_account_id, _elt_endpoint, _elt_insert_timestamp, _elt_update_timestamp, _gcs_file_path
    )
    VALUES (
        S.account_id, S.account_name, S.account_number, S.account_status,
        S.campaign_id, S.campaign_name, S.campaign_status,
        S.ad_group_id, S.ad_group_name, S.ad_group_status,
        S.keyword, S.keyword_id, S.keyword_status,
        S.goal, S.goal_id, S.goal_type,
        S.marketing_objective, S.channel, S.sub_channel,
        S.time_period, S.aggregation_type,
        S.impressions, S.clicks, S.ctr, S.spend,
        S.conversions_qualified, S.conversion_rate, S.cost_per_conversion,
        S.all_conversions_qualified, S.all_conversion_rate, S.all_cost_per_conversion,
        S.all_return_on_ad_spend, S.all_revenue, S.all_revenue_per_conversion,
        S.return_on_ad_spend, S.revenue, S.revenue_per_conversion,
        S.assists, S.cost_per_assist, S.revenue_per_assist,
        S.view_through_conversions_qualified, S.view_through_revenue,
        S._elt_account_id, S._elt_endpoint, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S._gcs_file_path
    );
