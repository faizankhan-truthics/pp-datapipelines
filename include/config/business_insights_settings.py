# include/config/business_insights_settings.py
import os
import re

from airflow.models import Variable

BQ_LOCATION = "US"
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
BQ_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GOLD_DATASET = Variable.get("BQ_GOLD_DATASET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "sql", "business_insights")

# Where reports go is decided purely by the deployment's environment variables: dev and prod run the same
# code, and each is given its own channel. Delivery is by Slack bot token (chat:write) posting to a channel ID:
# a short main message with the charts in a thread under it.
SLACK_BOT_TOKEN = os.environ.get("BUSINESS_INSIGHTS_SLACK_BOT_TOKEN")
CHANNEL_ID = os.environ.get("BUSINESS_INSIGHTS_CHANNEL_ID")


def _slug(product: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", product).strip("_").upper()


def product_channel_env_var(product: str) -> str:
    """Optional per-product channel ID, e.g. 'QR' -> BUSINESS_INSIGHTS_CHANNEL_ID__QR."""
    return f"BUSINESS_INSIGHTS_CHANNEL_ID__{_slug(product)}"


def channel_for(product: str) -> str | None:
    return os.environ.get(product_channel_env_var(product)) or CHANNEL_ID


# Hand-maintained product definitions, matching how the Executive dashboards filter: a product
# is a SET of PSP accounts (names as in *_mrr_waterfall_daily.account_name). Accounts claimed here
# are removed from the data-derived products so nothing is reported twice. Products not listed
# fall back to dim_product_payment_map names. Groups confirmed in Slack by Eugenia Dorn and Alex Bogdan
# (2026-10-19); QR confirmed earlier from the Executive dashboard queries.
PRODUCT_ACCOUNT_GROUPS: dict[str, dict[str, list[str]]] = {
    "QR": {
        "solidgate": ["QR_PROD_QGA_LIVE"],
        "stripe": ["OQG", "QCI", "OQG LLC"],
    },
    "Infochecker": {
        "solidgate": ["PS_PROD_INFOCHECKER"],
        "stripe": ["InfoChecker"],
    },
    "Location-Tool": {
        "solidgate": ["PS_PROD_LOCATIONTOOL_LIVE"],
        "stripe": ["LT"],
    },
    "Tracelo": {
        "solidgate": ["PS_PROD_TRACELO_LIVE"],
        "stripe": ["TRC", "Tracelo (rejected)"],
    },
    "Chatbox": {
        "solidgate": ["CHATBOX_PROD"],
        "stripe": ["Chat-Box"],
    },
    # IC-01..IC-08 are the warehouse's current names for the Stripe accounts now called IQ-xx;
    # they belong to IQ Center (confirmed by Eugenia Dorn). Add the IQ-xx names once they sync.
    "IQ Center": {
        "solidgate": ["QUIZ_PROD_IQCENTER_LIVE"],
        "stripe": ["IQ Center", "IQ-01"] + [f"IC-0{i}" for i in range(1, 9)],
    },
    "Public Registry": {
        "solidgate": ["BGR_PROD_PUBLICREGISTRY_US"],
        "stripe": ["Publicregistry.us"],
    },
    "Reversly": {
        "solidgate": ["REVERSELY_LIVE"],
        "stripe": ["reversly.com"],
    },
    "Simpl.ai": {
        "solidgate": ["CHATBOT_PROD_SIMPL"],
        "stripe": ["Simpl.ai"],
    },
    "Files Editor": {
        "solidgate": ["PDF_PROD_FILES_EDITOR"],
        "stripe": ["Files-Editor"] + [f"FE-{i:02d}" for i in range(2, 17)],
    },
    # MSC_PROD is GiftMusic (confirmed by Eugenia Dorn).
    "GiftMusic": {
        "solidgate": ["MSC_PROD"],
        "stripe": ["GiftMusic"],
    },
}
