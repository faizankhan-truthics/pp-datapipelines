{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'id',
    on_schema_change='append_new_columns',
    tags=["zendesk", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- Grain: one row per ticket comment. Drops html_body/plain_body (duplicates of body)
-- and via/attachments/translations (noisy JSON not needed downstream yet).

select
    id,
    ticket_id,
    audit_id,
    author_id,
    type,
    body,
    public,
    locale,
    outbound_locale,
    via_reference_id,
    created_at,
    _elt_zendesk_account_id,
    _elt_update_timestamp
from {{ source('bronze_zendesk', 'zendesk_ticket_comments') }}
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
