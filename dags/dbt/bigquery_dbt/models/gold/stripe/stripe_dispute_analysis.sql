{{ config(
    materialized='table',
    tags=["stripe", "gold", "daily"]
) }}

-- Dispute breakdown by date, capture week, status, network, RDR, reason code,
-- account and product. Full rebuild: dispute status changes over time.
with

disputes as (
    select
        created_date,
        captured_date,
        status,
        card_brand,
        rdr,
        network_reason_code,
        account_name,
        Product,
        currency,
        amount_usd
    from {{ ref('int_stripe_disputes') }}
    where created_date >= '2026-04-01'
),

normalized as (
    select
        created_date as dispute_date,
        date_trunc(captured_date, week) as captured_week,
        status,
        case
            when card_brand in ('visa', 'mastercard') then card_brand
            else 'others'
        end as card_brand,
        rdr,
        network_reason_code,
        account_name,
        Product,
        amount_usd
    from disputes
),

final as (
    select
        dispute_date,
        captured_week,
        date_trunc(dispute_date, month) as dispute_month,
        status,
        card_brand,
        rdr,
        network_reason_code,
        account_name,
        Product,
        amount_usd,
        count(*) as dispute_count
    from normalized
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

select * from final
