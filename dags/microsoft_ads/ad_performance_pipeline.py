"""Airflow DAG: microsoft_ads_ad_performance. Pipeline wiring is shared via
build_microsoft_ads_report_dag. Phase 1, entity #4 of 5 (Account/Campaign/AdGroup/Ad/
Keyword performance).

Column list below is the full AdPerformanceReportColumn enum (95 values) minus four
categories of exclusions -- see
https://learn.microsoft.com/en-us/advertising/reporting-service/adperformancereportcolumn:

1. Hourly-incompatible (2 columns) -- this report type has far fewer impression-share
   columns than Account/Campaign/AdGroup performance, and no quality-score family at all
   (no Historical* columns exist on this report), so this category is much smaller here:
   AbsoluteTopImpressionRatePercent, TopImpressionRatePercent.

2. Grouping dimensions (12 columns) -- including any of these makes Microsoft split one
   ad/time_period row into multiple rows (one per unique value of that attribute), which
   this table's grain (account_id x campaign_id x ad_group_id x ad_id x time_period)
   isn't built to hold -- same bug category found and fixed on every prior Microsoft Ads
   endpoint (see https://learn.microsoft.com/en-us/advertising/guides/reports#columnsdata):
   AdDistribution, Network, DeviceType, DeviceOS, Language, BidMatchType,
   DeliveredMatchType, Goal, GoalType, TopVsOther. GoalId is excluded too for the same
   reason it was on ad_group_performance -- same per-goal attribute as Goal/GoalType,
   not worth risking the identical bug a third time even though it's not explicitly
   documented as a grouping column. DestinationUrl is excluded too (Copilot PR review
   catch, confirmed valid against Microsoft's own docs): it's reported *after*
   dynamic-text substitution, so the same ad can have different DestinationUrl values
   across different clicks/queries within the same hour -- the same row-splitting risk
   as every other grouping column here, just not one of the previously-known ones since
   no earlier Microsoft Ads report had this column at all. FinalUrl (reported *before*
   substitution, stable per ad) is kept instead and covers the same need.

3. Deprecated, permanently zero/empty (4 columns): AveragePosition, Conversions,
   AllConversions, ViewThroughConversions.

Everything else is included (77 columns) -- including the full set of ad-creative
attribute columns (AdTitle, AdDescription/AdDescription2, DisplayUrl,
FinalUrl/FinalMobileUrl/FinalAppUrl, TitlePart1-3, Headline/LongHeadline, BusinessName,
Path1/Path2, AdLabels, AdStrength, AdStrengthActionItems), since these describe the ad's
actual content and nothing about them is technically broken for this report type.

Applying the %-suffix lesson from ad_group_performance upfront this time instead of
waiting for a review comment to catch it again: Ctr, ConversionRate, and
AllConversionRate come back from Microsoft's CSV with a literal "%" suffix (e.g.
"12.50%"), which SAFE_CAST alone silently turns into NULL instead of erroring -- see
merge_ad_performance.sql, which wraps these three in REPLACE(..., '%', '') before
casting. This report has no Ptr (no call-extension metrics at the ad level), so only 3
columns need the fix here, not 4. VideoCompletionRate/ViewThroughRate were confirmed NOT
affected on ad_group_performance despite also being "Rate"-named -- not pre-emptively
fixing those here either, pending the same kind of real-data verification.
"""

from include.dag_factories.microsoft_ads_ingest import build_microsoft_ads_report_dag

AD_PERFORMANCE_COLUMNS = [
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
    "AdGroupStatus",
    "AdId",
    "AdType",
    "AdStatus",
    "AdLabels",
    "CurrencyCode",
    "CustomerId",
    "CustomerName",
    "MarketingObjective",
    "Channel",
    "SubChannel",
    "TrackingTemplate",
    "CustomParameters",
    "FinalUrlSuffix",
    # -- ad creative --
    "AdTitle",
    "AdDescription",
    "AdDescription2",
    "DisplayUrl",
    "FinalUrl",
    "FinalMobileUrl",
    "FinalAppUrl",
    "TitlePart1",
    "TitlePart2",
    "TitlePart3",
    "Headline",
    "LongHeadline",
    "BusinessName",
    "Path1",
    "Path2",
    "AdStrength",
    "AdStrengthActionItems",
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
]

build_microsoft_ads_report_dag(
    entity="ad_performance",
    report_type="AdPerformanceReportRequest",
    columns=AD_PERFORMANCE_COLUMNS,
    bronze_asset_uri="data://bronze/microsoft_ads/ad_performance",
    schedule="55 3 * * *",
    aggregation="Hourly",
)
