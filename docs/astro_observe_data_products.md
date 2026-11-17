# Astro Observe — Data Products & Assets (gold layer)

Reference for what is monitored in Astro Observe: every `data-499514.gold.*` table and the Data Product it belongs to.

**Last updated: 2026-10-15**

- **7 Data Products**, **49 assets**. That covers every model in `dags/dbt/bigquery_dbt/models/gold/`.
- Owner of all products: **Pankaj Gawade**

## SLA (same for every product)

| Setting | Value |
|---|---|
| Type | Freshness |
| Schedule | Daily, **06:30 AM UTC** |
| Lookback | **8 hours**: an asset is a Hit if it was refreshed between 22:30 and 06:30 UTC |
| Refreshed by | `dbt_dag` ([dags/dbt/dbt_dag.py](../dags/dbt/dbt_dag.py)). It starts when ingestion finishes, not at a fixed time, so its finish time varies day to day. |

## Summary

| # | Data Product | Assets | dbt folders |
|---|---|---|---|
| 1 | Payment Reconciliation | 3 | `gold/stripe/`, `gold/solidgate/` |
| 2 | Chargebacks & Refunds | 6 | `gold/solidgate/`, `gold/refunds/`, `gold/stripe/` |
| 3 | Revenue & MRR Reporting | 11 | `gold/net_revenue/`, `gold/mrr/`, `gold/stripe/`, `gold/solidgate/`, `gold/gross_profit/`, `gold/ga4_ads_recon/` |
| 4 | Marketing Attribution: GA4 to Payments | 7 | `gold/ga4/`, `gold/ga4_ads_recon/`, `gold/anytrack_recon/`, `gold/reference/` |
| 5 | VAMP – Card-Scheme Risk Monitoring | 13 | `gold/stripe/`, `gold/solidgate/`, `gold/vamp_initiative/` |
| 6 | Subscribers Reporting | 4 | `gold/subscribers/`, `gold/stripe/`, `gold/solidgate/` |
| 7 | User Journey & Identity | 5 | `gold/user_journey_mart/`, `gold/user_subscription_mart/` |
| | **Total** | **49** | |

All asset names below are in `data-499514.gold.`

---

## 1. Payment Reconciliation — 3 assets
*Reconciliation across charges, refunds, fraud warnings, disputes*

| Asset | Materialized | What it is |
|---|---|---|
| `stripe_monthly_recon` | table | Monthly counts and USD amounts of Stripe charges, refunds, fraud warnings and disputes. |
| `solidgate_card_orders_transaction_history` | view | One row per Solidgate card-order transaction attempt. |
| `solidgate_acceptance_decline_weekly` | table | Weekly card-order acceptance/decline counts by account, PSP, network, country, decline reason. Feeds the Acceptance Rate and Decline dashboards. |

## 2. Chargebacks & Refunds — 6 assets
*Chargeback and refund exposure across processors*

| Asset | Materialized | What it is |
|---|---|---|
| `solidgate_refund_rate` | table | Monthly Solidgate refund rate: refunded volume in month X / settled volume in month X. |
| `solidgate_chargebacks_transaction_history` | view | One row per Solidgate chargeback transaction. |
| `mastercard_refund_requests` | table | Support-ticket refund requests paid by Mastercard, matched to the Solidgate/Stripe order with refund outcome. |
| `mastercard_refund_summary_by_country` | table | Mastercard refund-request volume and approval rate by country. |
| `mastercard_refund_summary_by_product_account` | table | Mastercard refund-request volume and approval rate by product and Stripe account / Solidgate MID. |
| `stripe_dispute_analysis` | table | Stripe disputes since 2026-04-01 by date, network, RDR flag, reason code, account, product. |

## 3. Revenue & MRR Reporting — 11 assets
*Daily MRR/waterfall numbers Finance reports on*

| Asset | Materialized | What it is |
|---|---|---|
| `net_revenue_daily` | view | Unified daily net revenue across Stripe and Solidgate (USD), with no double counting between processors. |
| `solidgate_net_revenue_daily` | view | Daily Solidgate gross revenue, PSP fees, refunds, disputes, net revenue (USD). |
| `gross_profit_daily` | view | Daily gross profit per product = net revenue − ad spend − ChargebackHit cost. |
| `mrr_arr_daily` | view | Daily combined MRR and ARR (ARR = MRR × 12) across Solidgate and Stripe. |
| `stripe_mrr_usd_daily` | view | Daily Stripe MRR in USD. |
| `stripe_mrr_waterfall_daily` | view | Daily Stripe MRR waterfall (new, expansion, churn, …). |
| `stripe_mrr_by_channel_daily` | view | Daily Stripe MRR by acquisition channel. |
| `solidgate_mrr_usd_daily` | view | Daily Solidgate MRR in USD. |
| `solidgate_mrr_waterfall_daily` | view | Daily Solidgate MRR waterfall. |
| `solidgate_mrr_by_channel_daily` | view | Daily Solidgate MRR by acquisition channel. |
| `campaign_roas_daily` | view | Campaign ROAS against real collected Solidgate/Stripe revenue, campaign × day. |

## 4. Marketing Attribution: GA4 to Payments — 7 assets
*Ties GA4 sessions/transactions to actual Solidgate and Stripe payments*

| Asset | Materialized | What it is |
|---|---|---|
| `ga4_payment_match_daily` | view | Daily match rates between GA4 transaction ids and Solidgate/Stripe transactions, all accounts. |
| `ga4_payment_match_campaign_daily` | view | Same match at campaign × day. |
| `ga4_solidgate_match_daily` | view | Daily GA4 ↔ Solidgate match rates per product. |
| `ga4_transaction_attribution` | view | Transaction-level GA4 → Solidgate/Stripe attribution (campaign_id/gclid per subscription), used for LTV/CPA. |
| `ga4_ads_reconciliation` | view | GA4 ↔ Google Ads reconciliation at campaign × day. |
| `anytrack_reconciliation` | view | AnyTrack ↔ files_editor (Bravo DB) event-count reconciliation at day × event. |
| `dim_product_payment_map` | view | Reference: every PSP account → product → GA4 property. If this map breaks, all GA4 ↔ payment matching breaks. |

## 5. VAMP – Card-Scheme Risk Monitoring — 13 assets
*Visa/Mastercard excessive-chargeback-ratio tracking — compliance-critical*

| Asset | Materialized | What it is |
|---|---|---|
| `stripe_monthly_vamp` | table | Monthly Visa VAMP fraud/dispute ratios by statement descriptor. |
| `stripe_monthly_vamp_estimate` | incremental | End-of-month Visa VAMP count and ratio estimated from the month to date. |
| `stripe_vamp_dashboard_overview` | table | Rolling 5-month VAMP dashboard grain (account × month × descriptor), with end-of-month projection. |
| `stripe_emc_efm_report` | table | Mastercard ECM / EFM (Excessive Chargeback / Fraud Merchant) flags, rolling 6 months. |
| `stripe_smmp_refund_dispute_rate` | table | Mastercard SMMP refund and dispute rates per Stripe account, trailing 30 days. |
| `solidgate_daily_vamp` | table | Daily Visa VAMP ratios from Solidgate card orders. |
| `solidgate_monthly_vamp` | table | Monthly Visa VAMP ratios from Solidgate card orders. |
| `solidgate_smmp_acceptance_rate` | table | Mastercard SMMP 72h auth-rate monitor per account / PSP / MID with a RED flag. |
| `vamp_initiative_126_days` | table | Base table for the VAMP initiative dashboards: one row per subscription × invoice × payment attempt (InfoChecker Pro Access CE3). |
| `vamp_initiative_acceptance_rate` | view | Acceptance rate by retry attempt and invoice sequence number. |
| `vamp_initiative_efw` | view | Early fraud warning and non-RDR dispute rate by invoice sequence number. |
| `vamp_initiative_recovery_rate` | view | Recovery rate per invoice sequence number (amount recovered vs max attempted). |
| `vamp_initiative_trials_voiding` | table | Initiative #2: monthly impact of voiding trial auths (≤ $1.50): transactions and revenue removed, disputes avoided. |

## 6. Subscribers Reporting — 4 assets
*Paid / free / all subscribers, new / reactivated / churned movements, ARPA, ASP and LTV across Stripe and Solidgate. Backs the Subscribers dashboard.*

| Asset | Materialized | What it is |
|---|---|---|
| `combined_subscribers` | table | Dashboard dataset: Stripe + Solidgate merged at subscriber level, plus per-provider rows. |
| `stripe_subscribers` | table | Stripe subscriber metrics per product at day/week/month/quarter/year grain. |
| `solidgate_subscribers` | table | Solidgate subscriber metrics per product at day/week/month/quarter/year grain. |
| `solidgate_subscription_history` | view | Solidgate subscription versions with valid_from / valid_to. Input to subscriber counts. |

## 7. User Journey & Identity — 5 assets
*Resolved user identity and the visitor → signup → payer → renewal/churn journey across Amplitude, GA4, Solidgate, Stripe and Zendesk.*

| Asset | Materialized | What it is |
|---|---|---|
| `dim_user_journey` | table | One row per product × user (visitor, signup or payer) with first touch, stage timestamps and outcome. |
| `fct_user_journey_events` | table | Ordered event timeline per user across Amplitude, GA4, Solidgate, Stripe. |
| `user_identity_reconciliation_daily` | incremental | Daily snapshot of how well the sources reconcile: Amplitude vs GA4 counts, payer → app-user link rates, identity quality. |
| `dim_user` | view | Central user dimension (email-resolved, Stripe + Solidgate) with first-touch ad attribution and Zendesk ticket enrichment. |
| `fct_subscription` | view | Current-state subscription fact, one row per source_system × subscription_id. |

---

## Adding or changing a gold model

1. Every new model under `models/gold/` must be added to Astro Observe **in the same PR**. Add it to the matching Data Product, or create a new product if none fits.
2. Update this doc in the same PR: add the asset row and update the counts in **Summary**.
3. If a model is renamed or dropped, remove the old asset from Astro Observe and from this doc. Otherwise it will keep showing as a Miss.
4. If `dbt_dag` regularly finishes after 06:30 UTC, change the SLA here and in Astro Observe together.
