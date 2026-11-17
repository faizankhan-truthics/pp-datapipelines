{{ config(
    materialized='table',
    tags=["zendesk", "silver", "daily"]
    )
}}

-- Refund-request tickets, identified via Zendesk's AI intent tagging. Folds in three
-- tag variants: the strict/current intent, the "original" intent (kept even if the
-- ticket was later reclassified away from refund), and the mixed cancel+refund intent.
-- Grain: ticket_id.

with refund_tags as (
    select
        t.id as ticket_id,
        t.requester_id,
        t.created_at as ticket_created_at,
        tag
    from {{ source('bronze_zendesk', 'zendesk_tickets') }} t,
    unnest(json_extract_string_array(t.tags)) as tag
    where tag in (
        'intent__billing__refund__request',
        'original_intent__billing__refund__request',
        'intent__billing__wrong__cancel_amp_refund_custom'
    )
)

select distinct
    r.ticket_id,
    r.requester_id,
    r.ticket_created_at,
    lower(u.email) as email
from refund_tags r
join {{ source('bronze_zendesk', 'zendesk_users') }} u on u.id = r.requester_id
where u.email is not null
