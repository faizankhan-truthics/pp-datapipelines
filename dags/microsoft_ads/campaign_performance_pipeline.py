"""Airflow DAG: microsoft_ads_campaign_performance. Pipeline wiring is shared via
build_microsoft_ads_report_dag. Phase 1, entity #1 of 5 (Account/Campaign/AdGroup/Ad/
Keyword performance) -- see ~/Documents/claude-work-log/CONTEXT.md for the full roadmap.

Column list below is a first pass against CampaignPerformanceReportColumn -- validate
against a real sandbox report response (first run) before trusting it in prod; Microsoft's
reporting columns are picky about derived-type/impression-share combinations (see the
Report guide's "Column Restrictions" section) which aren't fully re-derived here yet.

Deliberately excludes every column confirmed or likely to be a "grouping" attribute --
see https://learn.microsoft.com/en-us/advertising/guides/reports#columnsdata: including an
attribute like DeviceType makes Microsoft return one row per unique value of that attribute
(e.g. one row for Computer, one for Smartphone) instead of one combined row per campaign/
time_period, which our merge SQL's grain (account_id x campaign_id x time_period) isn't
built to hold -- it would silently keep only one segment's numbers and discard the rest.
Excluded: AdDistribution, Network, DeviceType, DeviceOS, BidMatchType, DeliveredMatchType
(all explicitly called "attribute columns" in the Report guide's Column Restrictions
table), Goal, GoalType, BudgetName, BudgetStatus, BudgetAssociationStatus (explicitly
listed as grouping attributes for CampaignPerformanceReportRequest specifically in that
same table), and ConversionDelay (buckets conversions by days-to-convert, same category
of grouping dimension -- same mechanism as Google Ads' documented "conversion lag bucket"
concept). This pipeline is meant to be simple campaign-level totals per hour, not a
segmented report -- if segment-level breakdowns are ever needed, that's a new/wider grain,
not a tweak to this column list.
"""

from include.dag_factories.microsoft_ads_ingest import build_microsoft_ads_report_dag

CAMPAIGN_PERFORMANCE_COLUMNS = [
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
    "CampaignLabels",
    "CurrencyCode",
    "CustomerId",
    "CustomerName",
    "MarketingObjective",
    "Channel",
    "SubChannel",
    "TrackingTemplate",
    "CustomParameters",
    "FinalUrlSuffix",
    "BaseCampaignId",
    # -- core performance stats --
    # Deliberately excludes every impression-share/click-share/rank-share style column
    # (ImpressionSharePercent, ClickSharePercent, TopImpressionShare*, Absolute*,
    # AudienceImpression*, RelativeCtr, TopVsOther) -- these either error (code 2053) or
    # are silently ignored when combined with Hourly/HourOfDay aggregation (see
    # https://learn.microsoft.com/en-us/advertising/reporting-service/campaignperformancereportcolumn).
    # Also excludes Historical* columns (Daily-aggregation only) and deprecated columns
    # (AveragePosition, Conversions/AllConversions/ViewThroughConversions pre-Qualified,
    # LowQualityConversions pre-Qualified, NewCustomer*) in favor of their replacements.
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
    "UnknownCustomerConversions",
    "UnknownCustomerRevenue",
    # -- click quality --
    "LowQualityClicks",
    "LowQualityClicksPercent",
    "LowQualityImpressions",
    "LowQualityImpressionsPercent",
    "LowQualityConversionsQualified",
    "LowQualityGeneralClicks",
    "LowQualitySophisticatedClicks",
    # -- call extensions --
    "PhoneImpressions",
    "PhoneCalls",
    "Ptr",
    # -- quality score family -- static per-day scores; every hourly row on the same day
    # repeats the same value (not an error, just how Microsoft computes these).
    "QualityScore",
    "AdRelevance",
    "ExpectedCtr",
    "LandingPageExperience",
]

build_microsoft_ads_report_dag(
    entity="campaign_performance",
    report_type="CampaignPerformanceReportRequest",
    columns=CAMPAIGN_PERFORMANCE_COLUMNS,
    bronze_asset_uri="data://bronze/microsoft_ads/campaign_performance",
    schedule="15 3 * * *",
    aggregation="Hourly",
)
