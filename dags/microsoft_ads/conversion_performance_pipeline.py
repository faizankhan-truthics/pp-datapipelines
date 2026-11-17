"""Airflow DAG: microsoft_ads_conversion_performance. Phase 2, entity #1 -- a
per-conversion-goal view of the same account/campaign/ad group/keyword hierarchy
Phase 1 covers, not a new hierarchy level.

Full ConversionPerformanceReportColumn enum (45 values) minus: Conversions,
AllConversions, ViewThroughConversions (deprecated, always 0 -- the *Qualified
variants are kept instead), and DeviceType (grouping-dimension row-split risk, same
as every prior endpoint).

Goal/GoalId/GoalType are kept, not excluded as grouping dimensions -- that's the
point of this report. Grain widens to include goal_id, one level beyond
keyword_performance. Confirmed via real data (2026-11-12): Microsoft never actually
sends NULL for "no goal," it sends the literal value "0" -- the merge's
`IS NOT DISTINCT FROM` on goal_id is defensive, not solving an observed problem.

Confirmed via the first live run (2026-11-12): Keyword-level and Goal-level columns
coexist fine in one request, no column-combination error. Channel/SubChannel/
MarketingObjective re-verified safe against this endpoint's own staging data (not
just carried over from ad_group_performance).

Ctr/ConversionRate/AllConversionRate strip a literal "%" suffix before casting (see
merge_conversion_performance.sql). No Ptr on this report.
"""

from include.dag_factories.microsoft_ads_ingest import build_microsoft_ads_report_dag

CONVERSION_PERFORMANCE_COLUMNS = [
    # -- attributes/dimensions --
    "TimePeriod",
    "AccountId",
    "AccountName",
    "AccountNumber",
    "AccountStatus",
    "CampaignId",
    "CampaignName",
    "CampaignStatus",
    "AdGroupId",
    "AdGroupName",
    "AdGroupStatus",
    "Keyword",
    "KeywordId",
    "KeywordStatus",
    "Goal",
    "GoalId",
    "GoalType",
    "MarketingObjective",
    "Channel",
    "SubChannel",
    # -- core performance stats --
    "Impressions",
    "Clicks",
    "Ctr",
    "Spend",
    "ConversionsQualified",
    "ConversionRate",
    "CostPerConversion",
    "AllConversionsQualified",
    "AllConversionRate",
    "AllCostPerConversion",
    "AllReturnOnAdSpend",
    "AllRevenue",
    "AllRevenuePerConversion",
    "ReturnOnAdSpend",
    "Revenue",
    "RevenuePerConversion",
    "Assists",
    "CostPerAssist",
    "RevenuePerAssist",
    "ViewThroughConversionsQualified",
    "ViewThroughRevenue",
]

build_microsoft_ads_report_dag(
    entity="conversion_performance",
    report_type="ConversionPerformanceReportRequest",
    columns=CONVERSION_PERFORMANCE_COLUMNS,
    bronze_asset_uri="data://bronze/microsoft_ads/conversion_performance",
    schedule="5 4 * * *",
    aggregation="Hourly",
)
