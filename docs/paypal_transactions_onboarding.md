# PayPal sandbox transactions ingestion

The `paypal_transactions_pipeline` DAG retrieves sandbox transactions from PayPal's
Transaction Search API (`GET /v1/reporting/transactions`) and writes JSON landing
files and Avro release files to GCS before merging typed records into the
`paypal_transactions` BigQuery bronze table.

## Sandbox setup

1. Create a PayPal sandbox REST app and enable Transaction Search API access.
2. Provide the app credentials to the Airflow worker as environment variables:
   `PAYPAL_SANDBOX_CLIENT_ID` and `PAYPAL_SANDBOX_CLIENT_SECRET`.
3. Ensure the Airflow Variables `GCS_LANDING_BUCKET`, `GCP_PROJECT_ID`,
   `BQ_STAGING_DATASET`, and `BQ_BRONZE_DATASET` are set, and that
   `GCP_CONN_ID` points to a service account with GCS and BigQuery access.

The API host is fixed to `https://api-m.sandbox.paypal.com`; this pipeline does not
send requests to PayPal production.

## Backfills and dbt

The DAG accepts `date_from` and `date_to` as UTC ISO-8601 values in the run
configuration. If omitted, it processes the Airflow data interval. API requests are
split into windows of at most 30 days.

The bronze source is declared in `models/bronze/paypal.yml`. The
`int_paypal_transactions` silver model exposes typed transaction dates, amounts,
currencies, and status; amounts are not converted to another currency.
