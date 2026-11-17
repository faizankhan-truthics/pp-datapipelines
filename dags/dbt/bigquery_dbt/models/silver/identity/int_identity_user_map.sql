{{ config(
    materialized='table',
    tags=["identity", "silver", "daily"],
) }}

-- Cross-source identity resolution: maps every (source_system, source_account,
-- source_customer_id) to a single user_key, matched purely on lower-cased/trimmed
-- email — the only key that exists in more than one of these systems today.
-- No fuzzy matching: two customers sharing an email (e.g. a shared inbox) will
-- collapse into one user_key, and a customer with no email cannot be matched at all
-- (user_key is null, match_method = 'unmatched'). Consumers should treat user_key as
-- best-effort identity, not a verified single-human guarantee.
-- Grain: one row per (source_system, source_account, source_customer_id).

with solidgate_customers as (

    select
        customer_account_id as source_customer_id,
        customer_email      as email_raw,
        _elt_account_name   as source_account,
        _elt_update_timestamp
    from {{ ref('int_solidgate_subscription_customers') }}
    where customer_account_id is not null

),

-- the same customer_account_id can appear on multiple subscriptions; keep the email
-- last seen on file for that customer.
solidgate_latest as (

    select * except (rn)
    from (
        select
            *,
            row_number() over (
                partition by source_customer_id, source_account
                order by _elt_update_timestamp desc
            ) as rn
        from solidgate_customers
    )
    where rn = 1

),

stripe_customers as (

    -- int_stripe_customers is already unique on (id, _elt_stripe_account_id).
    select
        id                     as source_customer_id,
        email                  as email_raw,
        _elt_stripe_account_id as source_account,
        _elt_update_timestamp
    from {{ ref('int_stripe_customers') }}

),

combined as (

    select 'solidgate' as source_system, source_customer_id, source_account, email_raw, _elt_update_timestamp
    from solidgate_latest

    union all

    select 'stripe' as source_system, source_customer_id, source_account, email_raw, _elt_update_timestamp
    from stripe_customers

)

select
    source_system,
    source_account,
    source_customer_id,
    email_raw,
    nullif(lower(trim(email_raw)), '') as email,
    case
        when nullif(lower(trim(email_raw)), '') is not null
            then to_hex(md5(lower(trim(email_raw))))
    end as user_key,
    case
        when nullif(lower(trim(email_raw)), '') is not null then 'email'
        else 'unmatched'
    end as match_method,
    _elt_update_timestamp
from combined
