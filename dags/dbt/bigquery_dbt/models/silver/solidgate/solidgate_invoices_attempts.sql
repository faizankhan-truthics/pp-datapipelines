{{ config(
    materialized='table',
    tags=["solidgate", "silver", "daily"],
    ) }}

-- Invoice x order (payment attempt) grain, for downstream retry-attempt
-- calculations. Thin projection over int_solidgate_subscription_invoice_orders
-- rather than re-shredding the subscriptions payload: order_id is only unique
-- within an account's own namespace, so subscription_id/_elt_account_name are
-- kept alongside it as join keys.

select
    subscription_id,
    invoice_id,
    order_id,
    order_retry_attempt as retry_attempt,
    order_status        as status,
    order_operation     as operation,
    _elt_account_name
from {{ ref('int_solidgate_subscription_invoice_orders') }}
