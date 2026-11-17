# dags/business_insights/product_insights_dag.py
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

from include.config.business_insights_settings import (
    BQ_LOCATION,
    BQ_PROJECT_ID,
    GCP_CONN_ID,
    GOLD_DATASET,
    PRODUCT_ACCOUNT_GROUPS,
    SLACK_BOT_TOKEN,
    SQL_DIR,
    channel_for,
    product_channel_env_var,
)
from include.utils.business_insights import (
    deliver,
    fetch_account_rows,
    fetch_product_metrics,
    group_accounts,
)

TEMPLATE_PARAMS = {"bq_project": BQ_PROJECT_ID, "gold_dataset": GOLD_DATASET}


@dag(
    dag_id="product_business_insights",
    # Monday 08:00 UTC. Day/time still to be confirmed with the team.
    schedule="0 8 * * 1",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    # Stay paused until the output has been validated against the Executive dashboard.
    is_paused_upon_creation=True,
    default_args={"retries": 2, "pool": "standard_mem_pool"},
    tags=["business-insights", "slack", "gold"],
)
def product_business_insights():

    @task
    def list_products() -> list[dict]:
        """One entry per product: {"product": name, "accounts": {"solidgate": [...], "stripe": [...]}}."""
        rows = fetch_account_rows(
            sql_dir=Path(SQL_DIR),
            gcp_conn_id=GCP_CONN_ID,
            bq_location=BQ_LOCATION,
            template_params=TEMPLATE_PARAMS,
        )
        groups = group_accounts(rows, PRODUCT_ACCOUNT_GROUPS)
        return [{"product": name, "accounts": accts} for name, accts in groups.items()]

    @task(max_active_tis_per_dag=2)
    def post_product_insights(item: dict) -> None:
        product = item["product"]
        channel = channel_for(product)
        if not (SLACK_BOT_TOKEN and channel):
            # no channel (or token) configured for this product -> skip, don't fail the whole run
            print(
                f"No Slack destination ({product_channel_env_var(product)} or BUSINESS_INSIGHTS_CHANNEL_ID, "
                f"plus a bot token); skipping {product}."
            )
            return

        rows = fetch_product_metrics(
            sql_dir=Path(SQL_DIR),
            accounts=item["accounts"],
            gcp_conn_id=GCP_CONN_ID,
            bq_location=BQ_LOCATION,
            template_params=TEMPLATE_PARAMS,
        )
        if not rows:
            print(f"No metrics rows for {product}; nothing to send.")
            return

        deliver(product, rows, bot_token=SLACK_BOT_TOKEN, channel=channel)
        print(f"Delivered {product}.")

    post_product_insights.expand(item=list_products())


product_business_insights()
