"""Unit tests for include/config/chargebackhit_settings.py.

ENDPOINTS drives both the DAG (one extract/load/merge chain each) and the hook's
pagination (PAGINATION_STYLE[endpoint]), so enabling an endpoint without the other
pieces fails mid-run. These pin the cross-file invariants for every enabled endpoint.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from include.config import chargebackhit_settings as settings

REPO_SQL_DIR = Path(__file__).parents[3] / "include" / "sql" / "chargebackhit"


@pytest.mark.parametrize("endpoint", settings.ENDPOINTS)
def test_every_enabled_endpoint_has_a_known_pagination_style(endpoint):
    # chargebackhit_hook.py indexes PAGINATION_STYLE[endpoint] and branches on "offset".
    assert settings.PAGINATION_STYLE[endpoint] in {"offset", "token"}


@pytest.mark.parametrize("endpoint", settings.ENDPOINTS)
def test_every_enabled_endpoint_has_a_merge_sql_file(endpoint):
    assert (REPO_SQL_DIR / f"merge_{endpoint}.sql").is_file()


@pytest.mark.parametrize("endpoint", settings.ENDPOINTS)
def test_every_enabled_endpoint_has_date_filter_fields(endpoint):
    # chargebackhit_hook.py indexes DATE_FILTER_FIELDS[endpoint] to build the request filter.
    assert len(settings.DATE_FILTER_FIELDS[endpoint]) == 2


def test_parked_endpoints_stay_fully_configured():
    # Endpoints parked out of ENDPOINTS (e.g. "disputes") keep their per-endpoint entries so
    # re-enabling one is a one-line change -- the two maps must not drift apart.
    assert set(settings.PAGINATION_STYLE) == set(settings.DATE_FILTER_FIELDS)
    assert set(settings.ENDPOINTS) <= set(settings.PAGINATION_STYLE)


def test_endpoints_are_unique_and_non_empty():
    assert settings.ENDPOINTS
    assert len(settings.ENDPOINTS) == len(set(settings.ENDPOINTS))


def test_sql_dir_points_at_the_chargebackhit_sql_folder():
    assert Path(settings.SQL_DIR).parts[-3:] == ("include", "sql", "chargebackhit")


def test_avro_schema_carries_the_account_name():
    names = [f["name"] for f in settings.AVRO_SCHEMA["fields"]]

    assert "_elt_chargebackhit_account_name" in names
    assert "raw_data" in names
