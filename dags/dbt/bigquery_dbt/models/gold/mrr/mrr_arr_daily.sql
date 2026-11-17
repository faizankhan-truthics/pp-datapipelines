{{ config(
    tags=["mrr", "gold", "daily"],
) }}

with solidgate as (

    select
        snapshot_date,
        'solidgate' as payment_source,
        _elt_account_name as account_key,
        _elt_account_name as account_name,
        mrr_usd,
        round(mrr_usd * 12, 2) as arr_usd,
        active_subscriptions,
        customers,
        currencies
    from {{ ref('solidgate_mrr_usd_daily') }}

),

stripe as (

    select
        snapshot_date,
        'stripe' as payment_source,
        _elt_stripe_account_id as account_key,
        coalesce(account_name, _elt_stripe_account_id) as account_name,
        mrr_usd,
        round(mrr_usd * 12, 2) as arr_usd,
        active_subscriptions,
        customers,
        currencies
    from {{ ref('stripe_mrr_usd_daily') }}

)

select * from solidgate

union all

select * from stripe
