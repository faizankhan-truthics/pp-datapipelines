# Stripe product onboarding

Steps to onboard a new Stripe account, so that its data is ingested and it shows up under the right name and product in silver and gold.

**Last updated: 2026-11-03**

There are two parts:

1. **Ingestion.** Give Airflow the account's API key and account ID, so the Stripe DAGs pull its data.
2. **Mapping.** Add the account to `stripe_accounts.csv`, so dbt can attach an account name and product to its rows.

Do both, in both environments (sandbox and prod). If you skip the mapping, the account's data is still loaded, but `account_name` and `product` come through as NULL in every model that joins `stripe_accounts`.

---

## 1. Ingestion: API key and account ID

[include/config/stripe_settings.py](../include/config/stripe_settings.py) (`discover_stripe_accounts`) finds accounts from pairs of environment variables:

```
STRIPE_API_KEY_<SUFFIX>=rk_live_...
STRIPE_ACCOUNT_ID_<SUFFIX>=acct_...
```

- `<SUFFIX>` must be the same in both variables (e.g. `STRIPE_API_KEY_GIFTMUSIC` / `STRIPE_ACCOUNT_ID_GIFTMUSIC`). It becomes the lower-cased `account_name` used in GCS paths.
- If there is an API key with no matching `STRIPE_ACCOUNT_ID_<SUFFIX>`, the account is skipped and a warning is logged.
- `STRIPE_API_KEY_READ_ONLY` is excluded on purpose.
- Use a **restricted, read-only** key.

### Add the variables in Astro

Do this in **both** deployments, sandbox first, then prod.

**Astro UI**

1. Open the [Astro UI](https://cloud.astronomer.io)
2. Go to **Environment** → **Environment Variables** → **Edit Variables**.
3. Add two variables:

   | Key | Value | Secret |
   |---|---|---|
   | `STRIPE_API_KEY_<SUFFIX>` | the restricted key, `rk_live_...` | **Yes** |
   | `STRIPE_ACCOUNT_ID_<SUFFIX>` | `acct_...` | No |

4. Click **Save**. Astro restarts the deployment's components so the new variables take effect, which takes a few minutes. Don't trigger the DAGs until the restart has finished.

**Astro CLI** (same result)

```bash
astro deployment variable create --deployment-id <DEPLOYMENT_ID> \
  STRIPE_ACCOUNT_ID_<SUFFIX>=acct_...
astro deployment variable create --deployment-id <DEPLOYMENT_ID> --secret \
  STRIPE_API_KEY_<SUFFIX>=rk_live_...

# check (secret values are masked)
astro deployment variable list --deployment-id <DEPLOYMENT_ID>
```

Get `<DEPLOYMENT_ID>` from `astro deployment list`.

**Local**: once the variables are in Astro, export them into your local `.env` instead of typing them by hand:

```bash
astro env variable export --workspace-id cmr59jl2p26je01hxxbexlw6w --include-secrets > .env
astro dev restart
```

- `> .env` **overwrites** the whole file. Back up any local-only lines first, then add them back.
- `--include-secrets` writes the real key values into `.env`. Keep `.env` out of git: never commit it or share it.

Notes:
- Keep the same `<SUFFIX>` in sandbox and prod, so GCS paths and logs line up.
- Once a secret is saved it can't be read back in Astro. Keep the key in the team password manager.
- To check that the account was picked up, open the next `stripe_financial_pipeline` run: there should be a task / mapped instance for the new account, and no `Missing Stripe account ID` warning in the logs.

Once they are set, `stripe_financial_pipeline` and `stripe_events_pipeline` pick the account up on their next run, with no code change. To load history for the new account, trigger `stripe_financial_pipeline` with `date_from` / `date_to` and `run_type = backfill`.

---

## 2. Mapping: `stripe_accounts.csv`

### Where the file lives

| Environment | File | Loads into |
|---|---|---|
| Sandbox (dev) | `gs://bgr-datalake-bronze-sandbox/external/reference/stripe_accounts.csv` | `data-499514.bronze_sandbox.stripe_accounts` |
| Prod | `gs://bgr-datalake-bronze/external/reference/stripe_accounts.csv` | `data-499514.bronze.stripe_accounts` |

Keep the two files identical.

### Format

```csv
ID,Account,Product
acct_1U80L6Asx4j23u68,GiftMusic,giftmusic.com
```

| Column | Meaning | Rule |
|---|---|---|
| `ID` | Stripe account ID (`acct_...`) | Required, unique. Must equal `STRIPE_ACCOUNT_ID_<SUFFIX>`. |
| `Account` | Display name of the Stripe account | Required, unique (dbt `not_null` + `unique` tests in [models/bronze/stripe.yml](../dags/dbt/bigquery_dbt/models/bronze/stripe.yml)). Use the name finance uses, e.g. `FE-17`, `IC-08`. |
| `Product` | Product domain the account sells, e.g. `files-editor.com` | Several accounts can share one product. Use the same spelling as the existing rows for that product: it is the join key to the GA4 / PostHog / Amplitude seeds in `dim_product_payment_map`. |

The loader types every column as STRING and uses the header row as the column names (lower-cased in BigQuery: `id`, `account`, `product`). Don't rename or reorder the header.

### Steps

1. Download the current file from the **prod** bucket (GCS console → `external/reference/` → Download). Always edit the latest copy, because the load replaces the whole table.
2. Add one row per new account at the end of the file. Check that:
   - the `ID` isn't already in the file,
   - the `Account` name isn't already used,
   - `Product` matches the existing spelling if the product already exists.
3. Save as plain CSV (UTF-8, comma-separated, no quotes needed unless a value contains a comma). Excel sometimes adds a BOM or changes the delimiter, so check the file in a text editor.
4. Upload the file to `external/reference/` in **both** buckets, overwriting `stripe_accounts.csv`.
5. Run the load DAG in each environment (next section).
6. Check the load (see **Verify**).

---

## 3. Load into BigQuery: `gcs_external_to_bigquery_bronze`

DAG: [dags/external/external_files_to_bronze_dag.py](../dags/external/external_files_to_bronze_dag.py). It is triggered manually only (`schedule=None`).

- It loads **every** CSV under `gs://<GCS_LANDING_BUCKET>/external/reference/` into a bronze table of the same name (`stripe_accounts.csv` → `stripe_accounts`, `solidgate_mid_lookup.csv` → `solidgate_mid_lookup`).
- Each load is `WRITE_TRUNCATE`: the table is replaced with exactly what is in the file.
- The bucket and dataset come from the deployment's Airflow Variables (`GCS_LANDING_BUCKET`, `BQ_BRONZE_DATASET`). So triggering it in the sandbox deployment loads the sandbox file into `bronze_sandbox`, and triggering it in prod loads the prod file into `bronze`.

To run it, open Airflow in the deployment → `gcs_external_to_bigquery_bronze` → **Trigger**. No params are needed.

The new names reach silver and gold on the next `dbt_dag` run. Run it manually if you need them sooner.

---

## 4. Verify

```sql
-- the new rows are there; row count = data rows in the CSV
select count(*) from `data-499514.bronze.stripe_accounts`;
select * from `data-499514.bronze.stripe_accounts` where id in ('acct_...');

-- no Stripe charges left without a mapping (should return no rows)
select c._elt_stripe_account_id, count(*) as charges
from `data-499514.bronze.stripe_charges` c
left join `data-499514.bronze.stripe_accounts` a on a.id = c._elt_stripe_account_id
where a.id is null
group by 1;
```

Use `bronze_sandbox` in place of `bronze` to check sandbox.

## Downstream models that use the mapping

Silver: `int_stripe_charges`, `int_stripe_refunds`, `int_stripe_disputes`, `int_stripe_early_fraud_warnings`.
Gold: `dim_product_payment_map`, `net_revenue_daily`, `stripe_monthly_recon`, `stripe_monthly_vamp`, `stripe_mrr_usd_daily`, `stripe_mrr_waterfall_daily`, `mastercard_refund_requests`.

If the new product has a GA4 property, PostHog project or Amplitude project, also add it to the seeds in [dags/dbt/bigquery_dbt/seeds/](../dags/dbt/bigquery_dbt/seeds/) (`seed_ga4_payment_account_map`, `seed_amplitude_project_map`, `seed_product_registry`), using the same `Product` value.

If the product should appear in the dashboard's Project/Funnel filter, set `is_dashboard_project = true` on its `seed_ga4_payment_account_map` row and fill `project_name`, `product_type` (`quiz` or `search`) and `funnel_signal`. If it has no GA4 export yet, set `has_ga4_export = false` and give a `ga4_export_exclusion_reason`. `dim_project` and `int_ga4_funnel_sessions` read these rows, so there is no separate project seed. `stitch_exclusion_reason` is only about payment stitching, not the GA4 export.
