{{ config(
    materialized='table',
    tags=["identity", "silver", "daily"],
) }}

-- Where each customer came from, across both payment processors, keyed the same way as
-- int_identity_user_map so it joins straight onto dim_user / fct_subscription. The
-- attribution logic itself lives in the two source models (earliest GA4-matched payment,
-- persisted across renewals) -- this only standardizes their columns and attaches user_key.
-- Unattributed customers are kept, so anything summed off this still adds up to total.
-- Grain: source_system x source_account x source_customer_id.

with solidgate as (

    select
        'solidgate'                     as source_system,
        _elt_account_name               as source_account,
        customer_account_id             as source_customer_id,
        source,
        medium,
        campaign,
        campaign_id,
        gclid,
        attribution_status,
        attributed_from_subscription_id,
        attributed_from_date
    from {{ ref('int_solidgate_customer_attribution') }}

),

stripe as (

    select
        'stripe'                        as source_system,
        _elt_stripe_account_id          as source_account,
        customer                        as source_customer_id,
        source,
        medium,
        campaign,
        campaign_id,
        gclid,
        attribution_status,
        attributed_from_subscription_id,
        attributed_from_date
    from {{ ref('int_stripe_customer_attribution') }}

),

unioned as (

    select * from solidgate
    union all
    select * from stripe

)

select
    u.source_system,
    u.source_account,
    u.source_customer_id,
    -- null when the customer record had no matchable email (see int_identity_user_map)
    im.user_key,
    u.source,
    u.medium,
    u.campaign,
    u.campaign_id,
    u.gclid,
    u.attribution_status,
    u.attributed_from_subscription_id,
    u.attributed_from_date
from unioned u
left join {{ ref('int_identity_user_map') }} im
    on  u.source_system      = im.source_system
    and u.source_account     = im.source_account
    and u.source_customer_id = im.source_customer_id
