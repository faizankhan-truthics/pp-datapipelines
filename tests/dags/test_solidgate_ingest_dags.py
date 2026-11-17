"""DAG-level tests for the six Solidgate ingest DAGs (dags/solidgate/*_dag.py).

The six DAGs share one builder (include/dag_factories/solidgate_ingest.py) and differ only
in the arguments each thin *_dag.py passes it: entity, hook class, land function, bronze
Asset, and (for subscriptions) an extra release-history load. Wrong arguments are the main
DAG-level risk: a DAG that builds the wrong hook
(card_orders' endpoint landed as chargebacks), passes the wrong entity (wrong GCS folder /
merge SQL / staging table), or emits the wrong Asset (dbt never fires) still parses fine
and passes test_dag_example.py. Shared building blocks are tested elsewhere (hooks:
tests/include/custom_hooks/solidgate/, land/release: test_solidgate_to_gcs_operator.py,
windowing/accounts: test_solidgate_settings.py, merge SQL: tests/sql/), so these tests
pin only the per-DAG wiring and the DAG-local task logic (ingest + get_release_paths).

Accounts are injected via AIRFLOW_VAR_SOLIDGATE_ACCOUNTS so the parsed task list doesn't
depend on whichever real Solidgate credentials happen to be in the container's .env.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pendulum
import pytest
from airflow.exceptions import AirflowSkipException
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowFailException

from include.config.solidgate_settings import SQL_DIR
from include.custom_hooks.solidgate.apm_orders import ApmOrdersHook
from include.custom_hooks.solidgate.card_orders import CardOrdersHook
from include.custom_hooks.solidgate.chargebacks import ChargebacksHook
from include.custom_hooks.solidgate.financial_entries import FinancialEntriesHook
from include.custom_hooks.solidgate.fraud_alerts import FraudAlertsHook
from include.custom_hooks.solidgate.subscriptions import SubscriptionsHook

SOLIDGATE_DAG_DIR = Path(__file__).parents[2] / "dags" / "solidgate"

ACCOUNTS = [
    {
        "account_name": "acme",
        "public_key_env": "SOLIDGATE_PUBLIC_KEY_acme",
        "secret_key_env": "SOLIDGATE_SECRET_KEY_acme",
    },
    {
        "account_name": "zebra",
        "public_key_env": "SOLIDGATE_PUBLIC_KEY_zebra",
        "secret_key_env": "SOLIDGATE_SECRET_KEY_zebra",
    },
]

# entity -> (hook class, land function name in the DAG module, bronze Asset URI the dbt DAG waits on)
ENTITIES = {
    "apm_orders": (
        ApmOrdersHook,
        "land_and_release_to_gcs",
        "solidgate://financial/apm/bronze",
    ),
    "card_orders": (
        CardOrdersHook,
        "land_and_release_to_gcs",
        "solidgate://financial/card/bronze",
    ),
    "chargebacks": (
        ChargebacksHook,
        "land_and_release_to_gcs",
        "solidgate://financial/chargebacks/bronze",
    ),
    "financial_entries": (
        FinancialEntriesHook,
        "report_land_and_release_to_gcs",
        "solidgate://financial/entries/bronze",
    ),
    "fraud_alerts": (
        FraudAlertsHook,
        "land_and_release_to_gcs",
        "solidgate://financial/fraud/bronze",
    ),
    "subscriptions": (
        SubscriptionsHook,
        "land_and_release_to_gcs",
        "solidgate://financial/subscriptions/bronze",
    ),
}


@pytest.fixture(scope="module")
def solidgate_dags():
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AIRFLOW_VAR_SOLIDGATE_ACCOUNTS", json.dumps(ACCOUNTS))
        logger = logging.getLogger("airflow")
        old_value, logger.disabled = logger.disabled, True
        try:
            dag_bag = DagBag(dag_folder=str(SOLIDGATE_DAG_DIR), include_examples=False)
        finally:
            logger.disabled = old_value

    assert not dag_bag.import_errors, dag_bag.import_errors
    return dag_bag.dags


@pytest.fixture(params=sorted(ENTITIES))
def entity(request):
    return request.param


@pytest.fixture
def dag(solidgate_dags, entity):
    return solidgate_dags[f"solidgate_{entity}"]


class TestDagWiring:
    def test_one_ingest_task_per_account_all_feeding_get_release_paths_and_the_watcher(
        self, dag
    ):
        ingest_ids = sorted(
            t.task_id
            for t in dag.tasks
            if t.task_id.startswith("ingest_") and t.task_id != "ingest_failure_watcher"
        )

        assert ingest_ids == ["ingest_acme", "ingest_zebra"]
        for task_id in ingest_ids:
            assert dag.get_task(task_id).downstream_task_ids == {
                "get_release_paths",
                "ingest_failure_watcher",
            }

    def test_a_failed_account_fails_the_run_via_the_watcher_leaf(self, dag):
        # get_release_paths is all_done, so without a failing leaf a permanently failed
        # account would still end in a green run (bronze_ready succeeds for the others).
        watcher = dag.get_task("ingest_failure_watcher")

        assert watcher.trigger_rule == "one_failed"
        assert watcher.upstream_task_ids == {"ingest_acme", "ingest_zebra"}
        assert {t.task_id for t in dag.leaves} == {
            "bronze_ready",
            "ingest_failure_watcher",
        }

    def test_watcher_fails_without_retrying(self, dag):
        with pytest.raises(AirflowFailException, match="ingests failed"):
            dag.get_task("ingest_failure_watcher").python_callable()

    def test_release_paths_load_merge_bronze_chain(self, dag, entity):
        # get_release_paths is all_done so one failed account doesn't block loading the
        # others; bronze_ready is none_failed so a skipped load (no records) still emits.
        expected_downstream = {
            "get_release_paths": {"load_staging"},
            "load_staging": {"merge_bronze"},
            "merge_bronze": {"bronze_ready"},
            "bronze_ready": set(),
        }
        if entity == "subscriptions":
            expected_downstream["get_release_paths"] = {
                "load_staging",
                "append_release_history",
            }
            expected_downstream["append_release_history"] = {"bronze_ready"}

        for task_id, downstream in expected_downstream.items():
            assert dag.get_task(task_id).downstream_task_ids == downstream, task_id

        assert dag.get_task("get_release_paths").trigger_rule == "all_done"
        assert dag.get_task("bronze_ready").trigger_rule == "none_failed"

    def test_bronze_ready_emits_the_asset_the_dbt_dag_waits_on(self, dag, entity):
        emitted = {o.uri for t in dag.tasks for o in t.outlets}

        assert emitted == {ENTITIES[entity][2]}
        assert {o.uri for o in dag.get_task("bronze_ready").outlets} == {
            ENTITIES[entity][2]
        }

    def test_staging_table_and_merge_sql_are_scoped_to_this_entity(self, dag, entity):
        load_staging = dag.get_task("load_staging")
        merge_bronze = dag.get_task("merge_bronze")
        merge_sql = merge_bronze.configuration["query"]["query"]

        assert load_staging.destination_project_dataset_table.endswith(
            f".solidgate_{entity}"
        )
        assert load_staging.write_disposition == "WRITE_TRUNCATE"
        assert load_staging.source_format == "AVRO"
        assert merge_sql == f"merge_{entity}.sql"
        assert (SQL_DIR / merge_sql).is_file()
        assert merge_bronze.params["endpoint"] == entity

    def test_subscriptions_also_appends_every_release_to_history(self, solidgate_dags):
        dag = solidgate_dags["solidgate_subscriptions"]
        append = dag.get_task("append_release_history")

        assert append.write_disposition == "WRITE_APPEND"
        assert append.destination_project_dataset_table.endswith(
            ".solidgate_subscriptions_release_history"
        )

    def test_daily_schedule_without_catchup_and_one_run_at_a_time(self, dag):
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert dag.default_args["retries"] >= 2

    def test_runs_once_a_day(self, dag):
        minute, hour, *day_fields = dag.schedule.split()

        assert day_fields == ["*", "*", "*"]
        assert minute.isdigit() and hour.isdigit()

    def test_schedules_are_staggered_so_entities_dont_share_the_rate_limit_window(
        self, solidgate_dags
    ):
        schedules = [solidgate_dags[f"solidgate_{e}"].schedule for e in ENTITIES]

        assert len(set(schedules)) == len(schedules), schedules

    def test_task_defaults_pool_timeout_and_owner(self, dag):
        assert dag.default_args["pool"] == "standard_mem_pool"
        assert dag.default_args["execution_timeout"] == timedelta(minutes=120)
        assert dag.default_args["owner"] == "data-platform"

    def test_tagged_as_solidgate_ingestion(self, dag):
        assert {"ingestion", "solidgate", "bronze"} <= set(dag.tags)

    def test_params_default_to_a_scheduled_daily_run(self, dag):
        # Empty dates -> resolve_window falls back to the data interval; split_by_day on.
        assert dag.params["date_from"] == ""
        assert dag.params["date_to"] == ""
        assert dag.params["run_type"] == "daily"
        assert dag.params["split_by_day"] is True

    def test_merge_sql_is_resolvable_from_the_template_searchpath(self, dag, entity):
        assert any(
            (Path(p) / f"merge_{entity}.sql").is_file() for p in dag.template_searchpath
        )


def _ingest_context(*, accounts=None, split_by_day=True):
    return {
        "params": {
            "date_from": "2026-01-01 00:00:00",
            "date_to": "2026-01-03 00:00:00",
            "split_by_day": split_by_day,
        },
        "dag_run": SimpleNamespace(conf={"accounts": accounts} if accounts else {}),
    }


def _patch_ingest(monkeypatch, task, entity, context):
    """Stub the context + land function in the DAG module's own globals; return the recorded calls."""
    dag_globals = task.python_callable.__globals__
    land_fn_name = ENTITIES[entity][1]
    calls = []

    def fake_land(**kwargs):
        calls.append(kwargs)
        return {"release_paths": ["release/solidgate/x/acme/2026-01-01/p.avro"]}

    monkeypatch.setitem(dag_globals, "get_current_context", lambda: context)
    monkeypatch.setitem(dag_globals, land_fn_name, fake_land)
    return calls


class TestIngestTask:
    def test_builds_this_entitys_hook_with_the_accounts_credentials(
        self, dag, entity, monkeypatch
    ):
        task = dag.get_task("ingest_zebra")
        calls = _patch_ingest(monkeypatch, task, entity, _ingest_context())

        result = task.python_callable()

        assert result == {
            "release_paths": ["release/solidgate/x/acme/2026-01-01/p.avro"]
        }
        assert len(calls) == 1
        hook = calls[0]["hook"]
        assert type(hook) is ENTITIES[entity][0]
        assert hook.public_key_env == "SOLIDGATE_PUBLIC_KEY_zebra"
        assert hook.secret_key_env == "SOLIDGATE_SECRET_KEY_zebra"

    def test_passes_entity_account_window_and_split_flag_to_the_land_function(
        self, dag, entity, monkeypatch
    ):
        task = dag.get_task("ingest_acme")
        calls = _patch_ingest(
            monkeypatch, task, entity, _ingest_context(split_by_day=False)
        )

        task.python_callable()

        kwargs = calls[0]
        assert kwargs["entity"] == entity
        assert kwargs["account_name"] == "acme"
        assert kwargs["date_from"] == "2026-01-01 00:00:00"
        assert kwargs["date_to"] == "2026-01-03 00:00:00"
        assert kwargs["split_by_day"] is False

    def test_scheduled_run_without_dates_ingests_the_previous_day_from_the_data_interval(
        self, dag, entity, monkeypatch
    ):
        task = dag.get_task("ingest_acme")
        context = {
            "params": {"date_from": "", "date_to": "", "split_by_day": True},
            "dag_run": SimpleNamespace(conf={}),
            "data_interval_start": pendulum.datetime(2026, 1, 1, tz="UTC"),
            "data_interval_end": pendulum.datetime(2026, 1, 2, tz="UTC"),
        }
        calls = _patch_ingest(monkeypatch, task, entity, context)

        task.python_callable()

        assert calls[0]["date_from"] == "2026-01-01 00:00:00"
        assert calls[0]["date_to"] == "2026-01-02 00:00:00"
        assert calls[0]["split_by_day"] is True

    def test_account_outside_the_conf_allowlist_is_skipped_without_calling_the_api(
        self, dag, entity, monkeypatch
    ):
        task = dag.get_task("ingest_zebra")
        calls = _patch_ingest(
            monkeypatch, task, entity, _ingest_context(accounts=["ACME"])
        )

        with pytest.raises(AirflowSkipException, match="zebra"):
            task.python_callable()

        assert calls == []

    def test_allowlist_match_is_case_insensitive(self, dag, entity, monkeypatch):
        task = dag.get_task("ingest_acme")
        calls = _patch_ingest(
            monkeypatch, task, entity, _ingest_context(accounts=["ACME"])
        )

        task.python_callable()

        assert len(calls) == 1


class TestGetReleasePaths:
    def test_collapses_page_files_to_one_wildcard_per_account_day_directory(self, dag):
        get_release_paths = dag.get_task("get_release_paths").python_callable
        results = [
            {
                "release_paths": [
                    "release/solidgate/e/acme/2026-01-01/a_00001.avro",
                    "release/solidgate/e/acme/2026-01-01/a_00002.avro",
                ]
            },
            {
                "release_paths": [
                    "release/solidgate/e/zebra/2026-01-01/z_00001.avro",
                    "release/solidgate/e/acme/2026-01-02/a_00001.avro",
                ]
            },
        ]

        assert get_release_paths(results) == [
            "release/solidgate/e/acme/2026-01-01/*.avro",
            "release/solidgate/e/acme/2026-01-02/*.avro",
            "release/solidgate/e/zebra/2026-01-01/*.avro",
        ]

    def test_failed_or_skipped_accounts_are_ignored(self, dag):
        # all_done: a failed/skipped ingest task pushes no XCom, so its slot is None.
        get_release_paths = dag.get_task("get_release_paths").python_callable

        assert get_release_paths(
            [None, {"release_paths": ["release/solidgate/e/acme/2026-01-01/a.avro"]}]
        ) == ["release/solidgate/e/acme/2026-01-01/*.avro"]

    @pytest.mark.parametrize(
        "results",
        [[], [None, None], [{"release_paths": []}, {}]],
        ids=["none", "all_failed", "no_pages"],
    )
    def test_nothing_to_load_skips_instead_of_loading_an_empty_source_list(
        self, dag, results
    ):
        get_release_paths = dag.get_task("get_release_paths").python_callable

        with pytest.raises(AirflowSkipException):
            get_release_paths(results)
