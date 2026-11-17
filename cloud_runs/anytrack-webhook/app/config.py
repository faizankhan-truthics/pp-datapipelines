import json
import os

ENVIRONMENT = os.environ.get("ENVIRONMENT", "sandbox")
GCS_BUCKET = os.environ["GCS_BUCKET"]

# None lets the storage client auto-detect (works on Cloud Run via the
# metadata server); set locally when running under user ADC, which doesn't
# always carry a project ID.
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")

# Label for which product group this deployment serves (e.g. "core",
# "files_editor", "tracelo") -- logging only, doesn't gate behavior. The
# same image gets deployed once per group; PRODUCT_GROUP + the products
# config below are the only things that differ between deployments.
PRODUCT_GROUP = os.environ.get("PRODUCT_GROUP", "default")

# {"<product>": "<webhook secret>"} for every product this deployment's
# group handles. In Cloud Run this is delivered via a Secret Manager
# reference ("Secrets exposed as environment variables"), never typed as a
# plaintext env var -- these are webhook secrets. See README for setup.
_ANYTRACK_PRODUCTS_CONFIG: dict[str, str] = json.loads(
    os.environ.get("ANYTRACK_PRODUCTS_CONFIG", "{}")
)

# Products this deployment answers for. Onboarding a product to an existing
# group means adding it to that group's Secret Manager secret -- no code
# change, no redeploy.
ANYTRACK_PRODUCTS = list(_ANYTRACK_PRODUCTS_CONFIG.keys())


def anytrack_expected_token(product: str) -> str | None:
    """Per-product webhook secret, sourced from this deployment's product group config."""
    return _ANYTRACK_PRODUCTS_CONFIG.get(product)
