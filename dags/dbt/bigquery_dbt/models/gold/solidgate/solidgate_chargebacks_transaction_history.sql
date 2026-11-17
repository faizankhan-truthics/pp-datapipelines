{{ config(
    materialized='view',
    tags=["solidgate", "gold", "daily"],
    description='Show all historical transactions of solidgate chargebacks'
) }}


select
    *
from {{ ref('int_solidgate_chargebacks_transactions')}}