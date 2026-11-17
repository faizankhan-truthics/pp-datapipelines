"""Airflow DAG: microsoft_ads_ad_group_performance. Pipeline wiring is shared via
build_microsoft_ads_report_dag. Phase 1, entity #3 of 5 (Account/Campaign/AdGroup/Ad/
Keyword performance).

Column list below is the full AdGroupPerformanceReportColumn enum (107 values) minus
four categories of exclusions -- see
https://learn.microsoft.com/en-us/advertising/reporting-service/adgroupperformancereportcolumn:

1. Hourly-incompatible (21 columns) -- every impression-share/click-share/rank-share
   style column either errors (code 2053) or silently returns garbage when combined
   with Hourly aggregation, same category as campaign_performance/account_performance,
   plus the full quality-score-history family, which a live test run (2026-10-28)
   confirmed via Microsoft's own API error is Daily-aggregation-only in its entirety --
   error code 2054, ErrorCode "InvalidGrainForHistoricalQualityScoreColumns": "ColumnXxx
   and Yyy columns are not available for the specified ReportAggregation", naming all
   four of HistoricalQualityScore/HistoricalExpectedCtr/HistoricalAdRelevance/
   HistoricalLandingPageExperience together (the docs for the latter two don't actually
   say this -- only HistoricalQualityScore/HistoricalExpectedCtr's own column
   descriptions mention Daily-only; the real constraint is wider than documented):
   ImpressionSharePercent, ImpressionLostToBudgetPercent, ImpressionLostToRankAggPercent,
   ClickSharePercent, AudienceImpressionSharePercent, AudienceImpressionLostToRankPercent,
   AudienceImpressionLostToBudgetPercent, RelativeCtr, ExactMatchImpressionSharePercent,
   AbsoluteTopImpressionRatePercent, AbsoluteTopImpressionShareLostToBudgetPercent,
   AbsoluteTopImpressionShareLostToRankPercent, AbsoluteTopImpressionSharePercent,
   TopImpressionRatePercent, TopImpressionShareLostToBudgetPercent,
   TopImpressionShareLostToRankPercent, TopImpressionSharePercent, HistoricalQualityScore,
   HistoricalExpectedCtr, HistoricalAdRelevance, HistoricalLandingPageExperience.

2. Grouping dimensions (11 columns) -- including any of these makes Microsoft split one
   ad_group/time_period row into multiple rows (one per unique value of that attribute),
   which this table's grain (account_id x campaign_id x ad_group_id x time_period) isn't
   built to hold -- same bug category found and fixed on campaign_performance/
   account_performance (see
   https://learn.microsoft.com/en-us/advertising/guides/reports#columnsdata):
   AdDistribution, Network, DeviceType, DeviceOS, Language, BidMatchType,
   DeliveredMatchType, Goal, GoalType, TopVsOther. GoalId is excluded too even though
   not explicitly documented as a grouping column -- it's the same per-goal attribute as
   Goal/GoalType, so it's treated the same way rather than risking the identical bug a
   third time.

3. Deprecated, permanently zero/empty (4 columns) -- superseded by their *Qualified
   replacements or explicitly discontinued: AveragePosition, Conversions, AllConversions,
   ViewThroughConversions.

4. Channel, SubChannel, MarketingObjective -- newer (Performance Max era) attribute
   columns with no documented grouping behavior either way, so flagged for verification
   rather than guessed at. Confirmed NOT grouping dimensions via a real run (2026-10-28,
   chatbox_ai, default 7-day/Hourly trigger): 116,114 rows = 116,114 distinct
   (ad_group_id, time_period) combinations exactly, zero duplicate (ad_group_id,
   time_period) pairs. Kept in.

Everything else is included (71 columns) -- including the current-value quality-score
columns (QualityScore, AdRelevance, ExpectedCtr, LandingPageExperience), since nothing
about those is broken for Hourly; same precedent as campaign_performance's quality-score
columns. Only the *Historical* variants are excluded, per the Hourly-incompatible
category above.
"""

from include.dag_factories.microsoft_ads_ingest import build_microsoft_ads_report_dag

AD_GROUP_PERFORMANCE_COLUMNS = [
    # -- attributes/dimensions --
    "TimePeriod",
    "AccountId",
    "AccountName",
    "AccountNumber",
    "AccountStatus",
    "CampaignId",
    "CampaignName",
    "CampaignStatus",
    "CampaignType",
    "BaseCampaignId",
    "AdGroupId",
    "AdGroupName",
    "AdGroupType",
    "AdGroupLabels",
    "Status",
    "CurrencyCode",
    "CustomerId",
    "CustomerName",
    "MarketingObjective",
    "Channel",
    "SubChannel",
    "TrackingTemplate",
    "CustomParameters",
    "FinalUrlSuffix",
    # -- core performance stats --
    "Impressions",
    "Clicks",
    "Ctr",
    "AverageCpc",
    "AverageCpm",
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
    # -- call extensions --
    "PhoneCalls",
    "PhoneImpressions",
    "Ptr",
    # -- quality score family -- static per-day scores; every hourly row on the same
    # day repeats the same value (not an error, just how Microsoft computes these).
    "QualityScore",
    "AdRelevance",
    "ExpectedCtr",
    "LandingPageExperience",
    # -- video --
    "AverageCPV",
    "AverageWatchTimePerImpression",
    "AverageWatchTimePerVideoView",
    "CompletedVideoViews",
    "TotalWatchTimeInMS",
    "VideoCompletionRate",
    "VideoViews",
    "VideoViewsAt25Percent",
    "VideoViewsAt50Percent",
    "VideoViewsAt75Percent",
    "ViewThroughRate",
    # -- app install / in-app purchase --
    "CostPerInstall",
    "CostPerSale",
    "Installs",
    "RevenuePerInstall",
    "RevenuePerSale",
    "Sales",
]

build_microsoft_ads_report_dag(
    entity="ad_group_performance",
    report_type="AdGroupPerformanceReportRequest",
    columns=AD_GROUP_PERFORMANCE_COLUMNS,
    bronze_asset_uri="data://bronze/microsoft_ads/ad_group_performance",
    schedule="50 3 * * *",
    aggregation="Hourly",
)
