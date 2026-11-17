"""Column-consistency tests for Microsoft Ads report pipelines.

Each entity's requested-columns list (e.g. CAMPAIGN_PERFORMANCE_COLUMNS) and the
CREATE TABLE in its merge_<entity>.sql have to be kept in sync by hand -- see each
pipeline file's and merge SQL's own comments for why (certain columns are deliberately
excluded for Hourly-compatibility/grouping/deprecation reasons, and the two lists must
still agree on everything that's left). This fails loudly if they ever drift apart,
instead of silently losing a column in bronze.

Parametrized over every entity so Ad Group/Ad/Keyword performance (Phase 1, entities
#3-5) just need one line added here when they land, not a new test file each.
"""

from __future__ import annotations

import re

import pytest

from dags.microsoft_ads.account_performance_pipeline import ACCOUNT_PERFORMANCE_COLUMNS
from dags.microsoft_ads.ad_group_performance_pipeline import (
    AD_GROUP_PERFORMANCE_COLUMNS,
)
from dags.microsoft_ads.ad_performance_pipeline import AD_PERFORMANCE_COLUMNS
from dags.microsoft_ads.campaign_performance_pipeline import (
    CAMPAIGN_PERFORMANCE_COLUMNS,
)
from dags.microsoft_ads.conversion_performance_pipeline import (
    CONVERSION_PERFORMANCE_COLUMNS,
)
from dags.microsoft_ads.keyword_performance_pipeline import (
    KEYWORD_PERFORMANCE_COLUMNS,
)
from include.config.microsoft_ads_settings import SQL_DIR

# Columns the merge SQL adds itself (ELT metadata not requested from Microsoft) --
# expected to exist in the SQL but never in the Python column list.
SQL_ONLY = {"aggregation_type"}

ENTITIES = {
    "campaign_performance": CAMPAIGN_PERFORMANCE_COLUMNS,
    "account_performance": ACCOUNT_PERFORMANCE_COLUMNS,
    "ad_group_performance": AD_GROUP_PERFORMANCE_COLUMNS,
    "ad_performance": AD_PERFORMANCE_COLUMNS,
    "keyword_performance": KEYWORD_PERFORMANCE_COLUMNS,
    "conversion_performance": CONVERSION_PERFORMANCE_COLUMNS,
}


def _norm(name: str) -> str:
    # CamelCase (Microsoft's column names) and snake_case (our BigQuery columns) both
    # collapse to the same string for comparison.
    return name.replace("_", "").lower()


def _sql_columns(entity: str) -> set[str]:
    sql = (SQL_DIR / f"merge_{entity}.sql").read_text()
    block = re.search(r"CREATE TABLE IF NOT EXISTS.*?\n\)", sql, re.S).group(0)
    cols = re.findall(
        r"^\s+(\w+)\s+(?:STRING|INT64|FLOAT64|TIMESTAMP|DATE)\b", block, re.M
    )
    return {c for c in cols if not c.startswith("_") and c not in SQL_ONLY}


@pytest.mark.parametrize("entity, requested_columns", ENTITIES.items())
def test_requested_columns_match_merge_sql(entity, requested_columns):
    requested = {_norm(c) for c in requested_columns}
    extracted = {_norm(c) for c in _sql_columns(entity)}

    assert requested - extracted == set(), (
        f"{entity}: requested from Microsoft but never extracted into bronze "
        f"(forgot to add to merge SQL?): {requested - extracted}"
    )
    assert extracted - requested == set(), (
        f"{entity}: extracted in merge SQL but never requested from Microsoft "
        f"(stale column, or missing from SQL_ONLY?): {extracted - requested}"
    )
