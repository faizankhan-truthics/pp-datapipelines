# PayPal transactions ingestion

The `paypal_transactions_pipeline` DAG retrieves transactions from PayPal's
Transaction Search API (`GET /v1/reporting/transactions`) for each configured
account. It writes JSON landing files and Avro release files to GCS before merging
typed records into the `paypal_transactions` BigQuery bronze table.

## Account setup

Each account needs its own PayPal REST app credentials and an explicit environment:
`sandbox` or `production`. The account name is used in GCS paths and
`_elt_account_name`, so give each configured account a unique name.

The recommended setup is the `PAYPAL_ACCOUNTS` Airflow Variable, containing JSON
with environment-variable names (never credential values), for example:

```json
[
  {
    "account_name": "sandbox",
    "environment": "sandbox",
    "client_id_env": "PAYPAL_SANDBOX_CLIENT_ID",
    "client_secret_env": "PAYPAL_SANDBOX_CLIENT_SECRET"
  },
  {
    "account_name": "production",
    "environment": "production",
    "client_id_env": "PAYPAL_CLIENT_ID_PRODUCTION",
    "client_secret_env": "PAYPAL_CLIENT_SECRET_PRODUCTION"
  }
]
```

Set the referenced environment variables securely in the Airflow deployment. The
API host is selected from the account's environment: sandbox uses
`https://api-m.sandbox.paypal.com`; production uses `https://api-m.paypal.com`.
Credentials are not copied between accounts or environments. Account names are
stored in `_elt_account_name`, and the selected API environment is stored in
`_elt_environment`.

Alternatively, accounts are auto-discovered from matching variable triples:
`PAYPAL_CLIENT_ID_<NAME>`, `PAYPAL_CLIENT_SECRET_<NAME>`, and
`PAYPAL_ENVIRONMENT_<NAME>` (where the environment is `sandbox` or `production`).

Ensure the Airflow Variables `GCS_LANDING_BUCKET`, `GCP_PROJECT_ID`,
`BQ_STAGING_DATASET`, and `BQ_BRONZE_DATASET` are set, and that `GCP_CONN_ID`
points to a service account with GCS and BigQuery access.

## Backfills and dbt

The DAG accepts `date_from` and `date_to` as UTC ISO-8601 values in the run
configuration. If omitted, it processes the Airflow data interval. API requests are
split into windows of at most 30 days.

The bronze source is declared in `models/bronze/paypal.yml`. The
`int_paypal_transactions` silver model exposes typed transaction dates, amounts,
currencies, and status; amounts are not converted to another currency.
