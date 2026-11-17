{{ config(
    materialized='table',
    tags=["refunds", "gold", "daily"],
    description='Mastercard refund-request volume, approval rate, and share of users by country. See mastercard_refund_requests for the caveat that country is not a consistent concept across source_system.'
) }}

with country_counts as (
    select
        source_system,
        country,
        count(distinct ticket_id) as users,
        count(distinct if(refund_issued, ticket_id, null)) as approved_refund_users
    from {{ ref('mastercard_refund_requests') }}
    group by 1, 2
)

select
    source_system,
    country,
    users,
    approved_refund_users,
    safe_divide(approved_refund_users, users) as approval_rate,
    safe_divide(users, sum(users) over (partition by source_system)) as share_of_users_in_source
from country_counts
