-- Every payment account in the product map, flagged by whether it has live MRR, with the product name
-- derived from ingested data. Python keeps the products that have at least one live account (with ALL their
-- accounts, so a retired account's history stays in the product) and applies the hand-maintained account
-- groups on top. account_name matches `account_name` in the *_mrr_waterfall_daily / mrr_arr_daily marts.
-- "Latest" is picked per payment source: Stripe and Solidgate build their date spines independently, so a
-- provider whose ingestion is a day behind must not lose all its accounts.
with latest_day as (

    select payment_source, max(snapshot_date) as snapshot_date
    from `{{ params.bq_project }}.{{ params.gold_dataset }}.mrr_arr_daily`
    group by payment_source

),

latest as (

    select d.payment_source, d.account_name, sum(d.mrr_usd) as mrr_usd
    from `{{ params.bq_project }}.{{ params.gold_dataset }}.mrr_arr_daily` d
    join latest_day using (payment_source, snapshot_date)
    group by d.payment_source, d.account_name

)

select
    lower(m.payment_provider)                    as provider,
    m.psp_account_name                           as account_name,
    coalesce(m.product_name, m.psp_account_name) as product_name,
    coalesce(l.mrr_usd, 0) > 0                   as is_live
from `{{ params.bq_project }}.{{ params.gold_dataset }}.dim_product_payment_map` m
left join latest l
    on l.payment_source = lower(m.payment_provider)
   and l.account_name = m.psp_account_name
order by coalesce(l.mrr_usd, 0) desc
