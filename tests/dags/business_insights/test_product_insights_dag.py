"""Tests for dags/business_insights/product_insights_dag.py (dag_id `product_business_insights`)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import dags.business_insights.product_insights_dag as mod

SQL_DIR = Path(__file__).parents[3] / "include" / "sql" / "business_insights"


@pytest.fixture(scope="module")
def dag():
    return mod.product_business_insights()


def test_dag_definition(dag):
    assert dag.dag_id == "product_business_insights"
    assert set(dag.task_dict) == {"list_products", "post_product_insights"}
    assert dag.schedule == "0 8 * * 1"
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True


def test_template_params_cover_every_placeholder_the_sql_uses():
    for sql_file in SQL_DIR.glob("*.sql"):
        used = set(
            re.findall(
                r"\{\{\s*params\.(\w+)\s*\}\}", sql_file.read_text(encoding="utf-8")
            )
        )
        assert used <= set(mod.TEMPLATE_PARAMS), sql_file.name


def test_metrics_sql_takes_accounts_as_query_parameters_not_template_values():
    sql = (SQL_DIR / "product_monthly_metrics.sql").read_text(encoding="utf-8")

    assert "@solidgate_accounts" in sql and "@stripe_accounts" in sql
    assert (
        "params.solidgate_accounts" not in sql and "params.stripe_accounts" not in sql
    )


def test_account_groups_only_name_known_providers():
    from include.config.business_insights_settings import PRODUCT_ACCOUNT_GROUPS

    for name, group in PRODUCT_ACCOUNT_GROUPS.items():
        assert set(group) <= {"solidgate", "stripe"}, name
