{{ config(
    materialized='table',
    tags=["refunds", "gold", "daily"],
    description='Mastercard refund-request volume and approval rate by product and processing account (Stripe account id / Solidgate MID).'
) }}

select
    source_system,
    account_id,
    account_name,
    product,
    count(distinct ticket_id) as mastercard_refund_request_users,
    count(distinct if(refund_issued, ticket_id, null)) as approved_refund_users,
    safe_divide(
        count(distinct if(refund_issued, ticket_id, null)),
        count(distinct ticket_id)
    ) as approval_rate
from {{ ref('mastercard_refund_requests') }}
group by 1, 2, 3, 4
