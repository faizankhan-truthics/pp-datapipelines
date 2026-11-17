"""Microsoft Advertising (Bing Ads) ingestion settings -- REST API, Phase 1.

Microsoft Advertising is migrating SOAP -> REST (feature freeze 2026-10-01, full SOAP
deprecation 2027-01-31 -- see https://learn.microsoft.com/en-us/advertising/guides/migrate-to-rest).
This pipeline targets the REST API directly, not SOAP.

Credentials follow the Stripe/Solidgate house style: separate per-field env vars, read
inside the task body (not at DAG-parse time, not via Variable.get) so secrets never show
up in the Airflow UI. See discover_microsoft_ads_accounts() for the per-account piece.

Auth is OAuth2 via Google (the Microsoft Advertising account here is Google-linked, not a
Microsoft/Entra identity -- see IdentityProvider in the hook). The developer token and
Google OAuth client/refresh token are account-wide, not per-account; only the
CustomerAccountId varies per account (one per product once products are mapped).
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
GCS_UPLOAD_CHUNK = 8 * 1024 * 1024  # 8 MiB streamed upload (memory-bounded)
BQ_LOCATION = "US"
SOURCE = "microsoft_ads"

# "production" or "sandbox" -- picks which REST host to call. Sandbox is used for local
# astro dev only (see MSADS_ACCOUNT_ID_SANDBOX in the dev deployment's env vars).
ENVIRONMENT = os.environ.get("MSADS_ENVIRONMENT", "production")

_REPORTING_HOST = {
    "production": "https://reporting.api.bingads.microsoft.com",
    "sandbox": "https://reporting.api.sandbox.bingads.microsoft.com",
}


def reporting_base_url() -> str:
    return _REPORTING_HOST[ENVIRONMENT]


# Google's token endpoint -- used to refresh the access token from the stored refresh
# token. Same endpoint regardless of production/sandbox; only the Microsoft Advertising
# side of the account differs between the two.
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

# Microsoft's ReportTimeZone enum has no literal UTC value (confirmed against the full
# enum -- only named city/region zones exist). GreenwichMeanTimeDublinEdinburghLisbonLondon
# is the closest match, but it observes British Summer Time (UTC+1) roughly late March ->
# late October -- report day boundaries will be off by an hour during that window versus
# true UTC. Accepted tradeoff: still far closer to UTC than the documented default
# (PacificTimeUSCanadaTijuana), and the 7-day lookback re-scan papers over an off-by-one-
# hour boundary anyway.
REPORT_TIME_ZONE = "GreenwichMeanTimeDublinEdinburghLisbonLondon"

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

# Reporting service's own documented concurrent-request throttle (error 207,
# ConcurrentRequestOverLimit) -- kept conservative since the real limit is undocumented.
RATE_LIMIT_PER_SECOND = 2

SQL_DIR = Path(__file__).parent.parent / "sql" / "microsoft_ads"

AVRO_SCHEMA = {
    "type": "record",
    "name": "MicrosoftAdsReleaseRecord",
    "fields": [
        {"name": "_elt_account_id", "type": "string"},
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        # Not part of the source CSV -- stamped by the operator from the Aggregation value
        # it requested (e.g. "daily", "hourly"), so mixed-grain reports stay self-describing
        # in bronze without parsing TimePeriod's shape or _elt_endpoint.
        {"name": "aggregation_type", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}


def discover_microsoft_ads_accounts() -> list[dict]:
    """Pairs every MSADS_ACCOUNT_ID_<NAME> env var into a discovered account.

    Unlike Stripe/Solidgate, the OAuth client/refresh token/developer token/customer id
    are shared across every account (one Microsoft Advertising login, one Customer/manager
    account, several advertiser AccountIds underneath it) -- only CustomerAccountId varies
    per account, so this only needs to pair one env var per account, not two.
    """
    accounts = []
    for key, value in os.environ.items():
        if key.startswith("MSADS_ACCOUNT_ID_") and value.strip():
            account_name = key.replace("MSADS_ACCOUNT_ID_", "", 1).lower()
            accounts.append(
                {
                    "account_name": account_name,
                    "account_id_env": key,
                    "account_id": value.strip(),
                }
            )
    if not accounts:
        raise RuntimeError(
            "No Microsoft Ads accounts configured: set at least one "
            "MSADS_ACCOUNT_ID_<NAME> env var (e.g. MSADS_ACCOUNT_ID_SANDBOX for local dev)."
        )
    return sorted(accounts, key=lambda a: a["account_name"])


def account_allowed(context, account_name: str) -> bool:
    """True if this account should run in this DAG run (conf['accounts'] allowlist)."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    requested = conf.get("accounts") if conf else None
    if not requested:
        return True
    return account_name.lower() in {a.lower() for a in requested}


def resolve_window(context) -> tuple[str, str, str]:
    """Resolve (ds, date_from, date_to) for an ingest run.

    Microsoft's Reporting API returns pre-aggregated numbers that can be retroactively
    corrected for up to a week after the fact (invalid-traffic credits -- see
    https://learn.microsoft.com/en-us/advertising/guides/reports#time-zones-in-reporting).
    There's no per-row updated_at/created_at to filter on the way Stripe/Solidgate do --
    the only way to catch those corrections is to re-pull a trailing window every run and
    let the bronze MERGE overwrite whatever changed. Default lookback mirrors
    files_editor_db's own trailing-window precedent.

    Backfill runs pass date_from/date_to via params/conf directly, same as every other
    source. Scheduled runs fall back to the data interval, widened backwards by
    MSADS_LOOKBACK_DAYS (default 7).
    """
    params = context["params"]
    date_from = params.get("date_from")
    date_to = params.get("date_to")
    if date_from and date_to:
        return date_from[:10], date_from, date_to

    end_dt = context["data_interval_end"].in_timezone("UTC").start_of("day")
    lookback_days = int(Variable.get("MSADS_LOOKBACK_DAYS", default_var="7"))
    start_dt = end_dt.subtract(days=lookback_days)

    return (
        end_dt.format("YYYY-MM-DD"),
        start_dt.format("YYYY-MM-DD HH:mm:ss"),
        end_dt.format("YYYY-MM-DD HH:mm:ss"),
    )
