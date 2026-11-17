{{ config(
tags=["ga4", "gold", "daily"],
) }}

-- Generalized version of infochecker_ga4_solidgate_match_daily: covers every GA4 property in
-- seed_ga4_payment_account_map, matched against its own payment source/account
with account_map as (

    select * from {{ ref('seed_ga4_payment_account_map') }}

),

ga4 as (

    select ga4_date, ga4_property_name, transaction_id
    from {{ ref('int_ga4_transactions') }}

),

solidgate_txns as (

    select
        sg_date              as pay_date,
        transaction_id,
        'solidgate'           as payment_source,
        _elt_account_name     as account_key
    from {{ ref('int_solidgate_transactions') }}

),

stripe_txns as (

    select
        stripe_date           as pay_date,
        transaction_id,
        'stripe'               as payment_source,
        _elt_stripe_account_id as account_key
    from {{ ref('int_stripe_transactions') }}

),

payment_txns as (

    select * from solidgate_txns
    union all
    select * from stripe_txns

),

-- Restrict both sides to accounts/properties present in account_map before the join below
payment_mapped as (

    select p.pay_date, p.transaction_id, p.payment_source, p.account_key
    from payment_txns p
    inner join account_map m
        on  p.payment_source = m.payment_source
        and p.account_key    = m.account_key

),

ga4_mapped as (

    select
        g.ga4_date,
        g.ga4_property_name,
        m.payment_source,
        m.account_key,
        g.transaction_id
    from ga4 g
    inner join account_map m
        on g.ga4_property_name = m.ga4_property_name

),

stitched as (

    select
        coalesce(p.payment_source, g.payment_source)   as payment_source,
        coalesce(p.account_key, g.account_key)         as account_key,
        coalesce(p.pay_date, g.ga4_date)                as pay_date,
        g.ga4_property_name,
        p.transaction_id                                as pay_txn_id,
        g.transaction_id                                as ga4_txn_id
    from ga4_mapped g
    full outer join payment_mapped p
        on  g.payment_source = p.payment_source
        and g.account_key    = p.account_key
        and g.transaction_id = p.transaction_id
        and g.ga4_date       = p.pay_date

),

match_daily as (

    select
        pay_date,
        payment_source,
        account_key,
        any_value(ga4_property_name)                                     as ga4_property_name,
        countif(pay_txn_id is not null)                                  as pay_txn_cnt,
        countif(ga4_txn_id is not null)                                  as ga4_txn_cnt,
        countif(pay_txn_id is not null and ga4_txn_id is not null)       as ga4_pay_matched,
        countif(pay_txn_id is not null and ga4_txn_id is null)           as pay_not_matched,
        round(safe_divide(
            countif(pay_txn_id is not null and ga4_txn_id is not null),
            countif(pay_txn_id is not null)
        ) * 100, 2) as pay_match_pct,
        round(safe_divide(
            countif(pay_txn_id is not null and ga4_txn_id is not null),
            countif(ga4_txn_id is not null)
        ) * 100, 2) as ga4_match_pct
    from stitched
    group by pay_date, payment_source, account_key

),

-- account_total_mrr_usd is the account's WHOLE daily MRR, not the portion tied to matched
-- transactions
account_mrr as (

    select
        'solidgate'           as payment_source,
        _elt_account_name     as account_key,
        snapshot_date,
        mrr_usd
    from {{ ref('solidgate_mrr_usd_daily') }}

    union all

    select
        'stripe'                as payment_source,
        _elt_stripe_account_id  as account_key,
        snapshot_date,
        mrr_usd
    from {{ ref('stripe_mrr_usd_daily') }}

)

select
    m.pay_date,
    m.ga4_property_name,
    m.payment_source,
    m.account_key,
    m.pay_txn_cnt,
    m.ga4_txn_cnt,
    m.ga4_pay_matched,
    m.pay_not_matched,
    m.pay_match_pct,
    m.ga4_match_pct,
    a.mrr_usd as account_total_mrr_usd
from match_daily m
left join account_mrr a
    on  m.payment_source = a.payment_source
    and m.pay_date        = a.snapshot_date
    and m.account_key      = a.account_key
