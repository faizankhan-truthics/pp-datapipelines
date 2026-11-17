"""Airflow DAG: microsoft_ads_keyword_performance. Pipeline wiring is shared via
build_microsoft_ads_report_dag. Phase 1, entity #5 of 5 (Account/Campaign/AdGroup/Ad/
Keyword performance) -- last one.

Column list below is the full KeywordPerformanceReportColumn enum (83 values) minus
four categories of exclusions -- see
https://learn.microsoft.com/en-us/advertising/reporting-service/keywordperformancereportcolumn:

1. Hourly-incompatible (6 columns): AbsoluteTopImpressionRatePercent,
   TopImpressionRatePercent, and all 4 Historical* columns (HistoricalQualityScore,
   HistoricalExpectedCtr, HistoricalAdRelevance, HistoricalLandingPageExperience) --
   only 2 of the Historical* columns are individually documented as Daily-only, but
   Microsoft's actual API error on ad_group_performance confirmed all 4 really are,
   regardless of what each column's own doc page says.

2. Grouping dimensions (13 columns) -- same bug category found and fixed on every
   prior Microsoft Ads endpoint (see
   https://learn.microsoft.com/en-us/advertising/guides/reports#columnsdata):
   AdDistribution, DeliveredMatchType, BidMatchType, DeviceType, DeviceOS, Language,
   Network, TopVsOther, Goal, GoalType, GoalId. AdId and AdType are excluded too
   (Copilot PR review catch, confirmed valid): an ad group can have multiple ads, and
   any of them can serve for the same keyword, so including AdId lets Microsoft split
   one keyword/hour into multiple rows (one per ad) -- this table's grain has no ad_id
   to hold that split, so it would silently undercount via the dedup.

3. Deprecated, permanently zero/empty (4 columns): AveragePosition, Conversions,
   AllConversions, ViewThroughConversions.

4. DestinationUrl (1 column) -- same row-split risk caught on ad_performance: reported
   *after* dynamic-text substitution, so the same keyword can have a different value
   per click/query within the same hour. FinalUrl (pre-substitution, stable) is kept
   instead.

Everything else is included (59 columns) -- keyword-specific attributes (Keyword,
KeywordStatus, KeywordLabels, BidStrategyType, CurrentMaxCpc, Mainline1Bid,
MainlineBid, FirstPageBid) and the current-value quality-score family (QualityScore,
ExpectedCtr, AdRelevance, LandingPageExperience, QualityImpact).

Ctr, ConversionRate, and AllConversionRate strip a literal "%" suffix before casting
(see merge_keyword_performance.sql) -- same fix applied on every endpoint since
ad_group_performance. No Ptr on this report (no call-extension metrics at keyword
level).
"""

from include.dag_factories.microsoft_ads_ingest import build_microsoft_ads_report_dag

KEYWORD_PERFORMANCE_COLUMNS = [
    # -- attributes/dimensions --
    "TimePeriod",
    "AccountId",
    "AccountName",
    "AccountNumber",
    "AccountStatus",
    "CampaignId",
    "CampaignName",
    "CampaignStatus",
    "BaseCampaignId",
    "AdGroupId",
    "AdGroupName",
    "AdGroupStatus",
    "Keyword",
    "KeywordId",
    "KeywordStatus",
    "KeywordLabels",
    "BidStrategyType",
    "CurrentMaxCpc",
    "Mainline1Bid",
    "MainlineBid",
    "FirstPageBid",
    "CurrencyCode",
    "MarketingObjective",
    "Channel",
    "SubChannel",
    "TrackingTemplate",
    "CustomParameters",
    "FinalUrl",
    "FinalMobileUrl",
    "FinalAppUrl",
    "FinalUrlSuffix",
    # -- quality score family -- static per-day scores; every hourly row on the same
    # day repeats the same value (not an error, just how Microsoft computes these).
    "QualityScore",
    "ExpectedCtr",
    "AdRelevance",
    "LandingPageExperience",
    "QualityImpact",
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
]

build_microsoft_ads_report_dag(
    entity="keyword_performance",
    report_type="KeywordPerformanceReportRequest",
    columns=KEYWORD_PERFORMANCE_COLUMNS,
    bronze_asset_uri="data://bronze/microsoft_ads/keyword_performance",
    schedule="0 4 * * *",
    aggregation="Hourly",
)
