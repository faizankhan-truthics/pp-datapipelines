from datetime import datetime
from pathlib import Path

from airflow.models.param import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from cosmos import (
    DbtDag,
    ExecutionConfig,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

from include.config.observability_settings import DBT_MARTS_READY_URI

# Bronze Assets that trigger dbt. Assets match by URI string, so each MUST stay identical to
# the outlet on its ingestion DAG (tests/dags/test_dbt_dag.py checks this).
STRIPE_BRONZE_DATASET = Asset("stripe://financial/bronze")
# stripe_events_pipeline (itself scheduled on STRIPE_BRONZE_DATASET); the stripe *_history
# models read stripe_events.
STRIPE_EVENTS_BRONZE_DATASET = Asset("stripe://events/bronze")
CHARGEBACKHIT_BRONZE_DATASET = Asset("data://bronze/chargebackhit")
FX_BRONZE_DATASET = Asset("data://bronze/exchange_rates/daily_fx")
GA4_BRONZE_DATASET = Asset("data://bronze/ga4/analytics")
SOLIDGATE_APM_BRONZE_DATASET = Asset("solidgate://financial/apm/bronze")
SOLIDGATE_CARD_BRONZE_DATASET = Asset("solidgate://financial/card/bronze")
SOLIDGATE_CHARGEBACKS_DATASET = Asset("solidgate://financial/chargebacks/bronze")
SOLIDGATE_FINANCIAL_ENTRIES_BRONZE_DATASET = Asset(
    "solidgate://financial/entries/bronze"
)
SOLIDGATE_FRAUD_ALERTS_BRONZE_DATASET = Asset("solidgate://financial/fraud/bronze")

SOLIDGATE_SUBSCRIPTIONS_BRONZE_DATASET = Asset(
    "solidgate://financial/subscriptions/bronze"
)
ZENDESK_BRONZE_DATASET = Asset("zendesk://financial/bronze")
# anytrack_recon silver/gold models read both of these.
ANYTRACK_BRONZE_DATASET = Asset("data://bronze/anytrack/events")
FILES_EDITOR_DB_BRONZE_DATASET = Asset("data://bronze/files_editor_db/billing")
# microsoft_ads_campaign_performance (Phase 1, entity #1 of 5).
MICROSOFT_ADS_CAMPAIGN_PERFORMANCE_BRONZE_DATASET = Asset(
    "data://bronze/microsoft_ads/campaign_performance"
)
# microsoft_ads_account_performance (Phase 1, entity #2 of 5).
MICROSOFT_ADS_ACCOUNT_PERFORMANCE_BRONZE_DATASET = Asset(
    "data://bronze/microsoft_ads/account_performance"
)
# microsoft_ads_ad_group_performance (Phase 1, entity #3 of 5).
MICROSOFT_ADS_AD_GROUP_PERFORMANCE_BRONZE_DATASET = Asset(
    "data://bronze/microsoft_ads/ad_group_performance"
)
# microsoft_ads_ad_performance (Phase 1, entity #4 of 5).
MICROSOFT_ADS_AD_PERFORMANCE_BRONZE_DATASET = Asset(
    "data://bronze/microsoft_ads/ad_performance"
)
# microsoft_ads_keyword_performance (Phase 1, entity #5 of 5 -- last one).
MICROSOFT_ADS_KEYWORD_PERFORMANCE_BRONZE_DATASET = Asset(
    "data://bronze/microsoft_ads/keyword_performance"
)
CHATBOX_DB_BRONZE_DATASET = Asset("data://bronze/chatbox_db/ai_response_activation")
# microsoft_ads_conversion_performance (Phase 2, first entity).
MICROSOFT_ADS_CONVERSION_PERFORMANCE_BRONZE_DATASET = Asset(
    "data://bronze/microsoft_ads/conversion_performance"
)

DBT_PROJECT_PATH = Path(__file__).resolve().parent / "bigquery_dbt"

profile_config = ProfileConfig(
    profile_name="bigquery_dbt",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp_connection_data_project",
        profile_args={
            "dataset": "dbt_analytics",
        },
    ),
)

my_cosmos_dag = DbtDag(
    dag_id="dbt_dag",
    project_config=ProjectConfig(
        dbt_project_path=str(DBT_PROJECT_PATH),
        install_dbt_deps=False,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    # No manifest_path / LoadMode.DBT_MANIFEST: that manifest was baked into the Docker
    # image at build time, so it only refreshed on a full image rebuild.
    # LoadMode.DBT_LS runs `dbt ls` against the live project on every DAG parse instead,
    # so the task graph always matches what's on disk regardless of deploy type.
    #
    # project_config.install_dbt_deps=False (above) stops Cosmos re-running `dbt deps` on
    # every parse.
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        exclude=["package:elementary"],
        # Tests spanning several models (e.g. test_customer_attribution_covers_mrr_customers)
        # get their own task downstream of ALL their parents. Without this Cosmos runs them
        # right after the first parent builds, against stale tables for the others.
        should_detach_multiple_parents_tests=True,
    ),
    params={
        "full_refresh": Param(
            default=False,
            type="boolean",
            title="Full Refresh",
            description="Enable to run dbt with --full-refresh. This rebuilds all incremental models and may take significantly longer than a normal run.",
        ),
        "run_type": Param(
            "daily",
            type="string",
            enum=["daily", "backfill"],
            title="Run type",
            description="Which Astro worker queue to run this DAG's tasks on.",
        ),
    },
    operator_args={
        "full_refresh": "{{ params.full_refresh }}",
        # Disable Elementary's on-run-end uploads here
        "vars": {
            "disable_dbt_artifacts_autoupload": True,
            "disable_run_results": True,
            "disable_tests_results": True,
            "disable_freshness_results": True,
            "disable_dbt_invocation_autoupload": True,
        },
    },
    # A list schedule is an AND: runs once every one of these has updated since the last run.
    schedule=[
        STRIPE_BRONZE_DATASET,
        STRIPE_EVENTS_BRONZE_DATASET,
        CHARGEBACKHIT_BRONZE_DATASET,
        FX_BRONZE_DATASET,
        GA4_BRONZE_DATASET,
        SOLIDGATE_APM_BRONZE_DATASET,
        SOLIDGATE_CARD_BRONZE_DATASET,
        SOLIDGATE_CHARGEBACKS_DATASET,
        SOLIDGATE_FINANCIAL_ENTRIES_BRONZE_DATASET,
        SOLIDGATE_FRAUD_ALERTS_BRONZE_DATASET,
        SOLIDGATE_SUBSCRIPTIONS_BRONZE_DATASET,
        ZENDESK_BRONZE_DATASET,
        ANYTRACK_BRONZE_DATASET,
        FILES_EDITOR_DB_BRONZE_DATASET,
        MICROSOFT_ADS_CAMPAIGN_PERFORMANCE_BRONZE_DATASET,
        MICROSOFT_ADS_ACCOUNT_PERFORMANCE_BRONZE_DATASET,
        MICROSOFT_ADS_AD_GROUP_PERFORMANCE_BRONZE_DATASET,
        MICROSOFT_ADS_AD_PERFORMANCE_BRONZE_DATASET,
        MICROSOFT_ADS_KEYWORD_PERFORMANCE_BRONZE_DATASET,
        CHATBOX_DB_BRONZE_DATASET,
        MICROSOFT_ADS_CONVERSION_PERFORMANCE_BRONZE_DATASET,
    ],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 2, "pool": "high_mem_pool"},
    tags=["dbt", "bigquery"],
)

# Emit an Airflow Asset once all dbt tasks finish, so the `dbt_observability` DAG (docs +
# Elementary report + Slack) and `daily_vamp_status` run right after this one instead of on
# an independent clock.
DBT_MARTS_READY = Asset(DBT_MARTS_READY_URI)


_leaves = list(my_cosmos_dag.leaves)
with my_cosmos_dag:
    _dbt_done = EmptyOperator(
        task_id="dbt_done", outlets=[DBT_MARTS_READY], trigger_rule="all_done"
    )
    for _leaf in _leaves:
        _leaf >> _dbt_done
