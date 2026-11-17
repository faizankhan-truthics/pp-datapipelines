{{ config(
    materialized='view',
    tags=["solidgate", "gold", "daily"],
    description='Historical Solidgate subscription lifecycle: one row per subscription version, with the period it was valid for.'
) }}

select
    *
from {{ ref('int_solidgate_subscription_history') }}
