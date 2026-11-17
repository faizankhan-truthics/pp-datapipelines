"""
Airflow hook for the Google Analytics 4 (GA4) Data API.

Wraps the `google-analytics-data` client and sources its credentials from an
Airflow Google Cloud connection (via `GoogleBaseHook`), so DAGs never have to
handle raw service account keys themselves.
"""

import logging

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

t_log = logging.getLogger("airflow.task")

# the GA4 Data API only accepts the analytics.readonly scope.
_GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


class GA4Hook(GoogleBaseHook):
    """
    Interact with the GA4 Data API using an Airflow Google Cloud connection.

    Authentication is delegated to `GoogleBaseHook`, so credentials never leave
    the connection. The built client is cached for the life of the hook.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        property_id: str | None = None,
        **kwargs,
    ) -> None:
        """
        Args:
            gcp_conn_id: The Airflow connection ID of type `google_cloud_platform`.
            property_id: Default GA4 property ID (numeric, e.g. "518695982") used
                by `run_report` when no per-call `property_id` is given.
        """

        super().__init__(gcp_conn_id=gcp_conn_id, **kwargs)
        self.property_id = property_id
        self._client: BetaAnalyticsDataClient | None = None

    def get_conn(self) -> BetaAnalyticsDataClient:
        """
        Return an authenticated GA4 Data API client, building it on first use.

        Credentials are resolved through the Google provider's `GoogleBaseHook`,
        so this never materialises the raw service account key itself.

        Returns:
            BetaAnalyticsDataClient: An authenticated GA4 Data API client.
        """

        if self._client is not None:
            return self._client

        credentials, project_id = self.get_credentials_and_project_id()

        # the GA4 Data API only accepts the analytics.readonly scope, so narrow
        # the connection's credentials to it explicitly rather than relying on
        # the connection's configured scope.
        if hasattr(credentials, "with_scopes"):
            credentials = credentials.with_scopes(_GA4_SCOPES)

        t_log.info(
            "Built GA4 client for service account %s (project %s).",
            getattr(credentials, "service_account_email", "unknown"),
            project_id,
        )

        self._client = BetaAnalyticsDataClient(credentials=credentials)
        return self._client

    def run_report(
        self,
        dimensions: list[str],
        metrics: list[str],
        start_date: str = "7daysAgo",
        end_date: str = "yesterday",
        property_id: str | None = None,
        return_metadata: bool = False,
        page_size: int = 100_000,
    ) -> list[dict] | tuple[list[dict], dict]:
        """
        Run a GA4 `runReport` request and return all rows, paging through the result set.

        GA4 caps a single `runReport` response at a bounded number of rows (and defaults to
        10,000 when no `limit` is given), so a wide date range or a high-cardinality dimension
        would silently truncate. This pages with `limit`/`offset` until the reported
        `row_count` is exhausted, so callers always get the complete result.

        Args:
            dimensions: GA4 dimension names, e.g. ["date"].
            metrics: GA4 metric names, e.g. ["activeUsers", "sessions"].
            start_date: Report start date (GA4 date or relative, default "7daysAgo").
            end_date: Report end date (GA4 date or relative, default "yesterday").
            property_id: GA4 property ID to query; falls back to the hook's
                `property_id` when omitted.
            return_metadata: When True, also return a metadata dict describing how
                GA4 produced the report (sampling, privacy thresholding, etc.).
            page_size: Rows requested per page (GA4 allows up to 250,000).
        Returns:
            list[dict]: One dict per row, keyed by dimension and metric name.
            tuple[list[dict], dict]: The rows plus the metadata dict, when
                `return_metadata` is True.
        """

        property_id = property_id or self.property_id
        if not property_id:
            raise ValueError("A GA4 property_id must be provided.")

        client = self.get_conn()

        t_log.info(
            "Requesting GA4 report for property %s (%s -> %s).",
            property_id,
            start_date,
            end_date,
        )

        rows: list[dict] = []
        offset = 0
        first_response = None

        while True:
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=page_size,
                offset=offset,
            )
            response = client.run_report(request)
            if first_response is None:
                first_response = (
                    response  # metadata + total row_count come from any page
                )

            for row in response.rows:
                record = {
                    dim.name: value.value
                    for dim, value in zip(request.dimensions, row.dimension_values)  # noqa: B905
                }
                record.update(
                    {
                        metric.name: value.value
                        for metric, value in zip(request.metrics, row.metric_values)  # noqa: B905
                    }
                )
                rows.append(record)

            page_rows = len(response.rows)
            offset += page_rows
            # response.row_count is the TOTAL matching rows, not just this page.
            if page_rows == 0 or offset >= (response.row_count or 0):
                break

        t_log.info("GA4 report returned %d rows.", len(rows))

        if not return_metadata:
            return rows

        # summarise the response metadata so callers can tell whether GA4 reduced
        # the numbers (sampling / privacy thresholding), which explains gaps when
        # reconciling against the GA4 UI.
        meta = first_response.metadata
        sampling = [
            {
                "samples_read_count": s.samples_read_count,
                "sampling_space_size": s.sampling_space_size,
            }
            for s in meta.sampling_metadatas
        ]
        metadata = {
            "subject_to_thresholding": meta.subject_to_thresholding,
            "sampled": bool(sampling),
            "sampling_metadatas": sampling,
            "data_loss_from_other_row": meta.data_loss_from_other_row,
            "currency_code": meta.currency_code,
            "time_zone": meta.time_zone,
            "empty_reason": meta.empty_reason,
            "row_count": len(rows),
        }

        return rows, metadata
