{{ config(
    materialized='view',
    tags=["solidgate", "gold", "daily"],
    description='Show all historical transactions of solidgate orders'
) }}


select
    *
from {{ ref('int_solidgate_card_orders_transactions')}}