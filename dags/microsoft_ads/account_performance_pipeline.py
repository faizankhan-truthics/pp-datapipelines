"""Airflow DAG: microsoft_ads_account_performance. Pipeline wiring is shared via
build_microsoft_ads_report_dag. Phase 1, entity #2 of 5 (Account/Campaign/AdGroup/Ad/
Keyword performance).

Column list below is the full AccountPerformanceReportColumn enum (111 values) minus
three categories of exclusions -- see
https://learn.microsoft.com/en-us/advertising/reporting-service/accountperformancereportcolumn:

1. Hourly-incompatible (16 columns) -- every impression-share/click-share/rank-share
   style column either errors (code 2053) or silently returns garbage when combined
   with Hourly aggregation, per Microsoft's own column-by-column documentation:
   ImpressionSharePercent, ImpressionLostToBudgetPercent, ImpressionLostToRankAggPercent,
   ClickSharePercent, AudienceImpressionSharePercent, AudienceImpressionLostToRankPercent,
   AudienceImpressionLostToBudgetPercent, AbsoluteTopImpressionRatePercent,
   AbsoluteTopImpressionShareLostToBudgetPercent, AbsoluteTopImpressionShareLostToRankPercent,
   AbsoluteTopImpressionSharePercent, ExactMatchImpressionSharePercent,
   TopImpressionRatePercent, TopImpressionShareLostToBudgetPercent,
   TopImpressionShareLostToRankPercent, TopImpressionSharePercent.

2. Grouping dimensions (10 columns) -- including any of these makes Microsoft split one
   account/time_period row into multiple rows (one per unique value of that attribute),
   which this table's grain (account_id x time_period) isn't built to hold -- same bug
   category found and fixed on campaign_performance (see
   https://learn.microsoft.com/en-us/advertising/guides/reports#columnsdata):
   AdDistribution, Network, DeviceType, DeviceOS, BidMatchType, DeliveredMatchType, Goal,
   GoalType, TopVsOther, ConversionDelay (buckets by days-to-convert, same category of
   grouping dimension).

3. Deprecated, permanently zero/empty (8 columns) -- superseded by their *Qualified
   replacements or explicitly discontinued: AveragePosition, Conversions, AllConversions,
   LowQualityConversions, ViewThroughConversions, NewCustomerConversionRate,
   NewCustomerCPA, NewCustomerReturnOnAdSpend.

Everything else is included (77 columns) -- unlike campaign_performance, this keeps
video and app-install/in-app-purchase metrics in, since nothing about them is technically
broken for this report type; they're just unused if the account doesn't run those
campaign types.
"""

from include.dag_factories.microsoft_ads_ingest import build_microsoft_ads_report_dag

ACCOUNT_PERFORMANCE_COLUMNS = [
    # -- attributes/dimensions --
    "TimePeriod",
    "AccountId",
    "AccountName",
    "AccountNumber",
    "AccountStatus",
    "CurrencyCode",
    "CustomerId",
    "CustomerName",
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
    "UnknownCustomerConversions",
    "UnknownCustomerRevenue",
    # -- click quality --
    "LowQualityClicks",
    "LowQualityClicksPercent",
    "LowQualityConversionRate",
    "LowQualityConversionsQualified",
    "LowQualityGeneralClicks",
    "LowQualityImpressions",
    "LowQualityImpressionsPercent",
    "LowQualitySophisticatedClicks",
    # -- call extensions --
    "PhoneCalls",
    "PhoneImpressions",
    "Ptr",
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
    "AppInstalls",
    "CostPerDownload",
    "CostPerInstall",
    "CostPerSale",
    "CPI",
    "CPP",
    "CPS",
    "Downloads",
    "Installs",
    "PostClickDownloadRate",
    "PostClickInstallRate",
    "PostInstallPurchaseRate",
    "PostInstallSubscriptionRate",
    "Purchases",
    "RevenuePerInstall",
    "RevenuePerSale",
    "Sales",
    "Subscriptions",
    # -- new customer (non-deprecated) --
    "NewCustomerConversions",
    "NewCustomerCount",
    "NewCustomerRevenue",
    "NewCustomerSpend",
]

build_microsoft_ads_report_dag(
    entity="account_performance",
    report_type="AccountPerformanceReportRequest",
    columns=ACCOUNT_PERFORMANCE_COLUMNS,
    bronze_asset_uri="data://bronze/microsoft_ads/account_performance",
    schedule="45 3 * * *",
    aggregation="Hourly",
)
