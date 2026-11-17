--view to dedupe charges to get one captured at date row
{{ config(
    materialized='view',
    tags=["stripe", "silver", "balance_transactions"],
    description='view to dedupe balance transactions to get one captured at date row'
) }}

select
    distinct
    source,
    created_ts,
    amount
from {{ source('bronze_stripe', 'stripe_balance_transactions') }}
where type = 'charge'