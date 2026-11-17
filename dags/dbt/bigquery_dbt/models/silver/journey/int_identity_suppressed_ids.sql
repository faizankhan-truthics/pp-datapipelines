{{ config(
    materialized='table',
    tags=["journey", "identity", "silver", "daily"]
) }}

-- Identifiers that point at more than one counterpart: a device shared by a household, an email
-- reused across accounts, a QA / test account. For links straight to an app user (E1, E2, E3, E6,
-- E9, E16) int_identity_resolution then attaches the id to nobody; for E7 / E12 it still attaches when
-- every counterpart resolves to the same user.
-- reason = 'shared' (2 .. identity_max_fanout counterparts) or 'super_node' (more), so the
-- reconciliation table can report both and over-merging stays visible.
-- Only the directions that would attach an id to a user are checked (e.g. device -> app user,
-- not app user -> devices: one person on 10+ browsers is normal on the web).
-- Grain: product x id_type x id_value x counterpart_type.

with edges as (

    select * from {{ ref('int_identity_edges') }}

),

fanout as (

    select
        product,
        id_type_a       as id_type,
        id_a            as id_value,
        id_type_b       as counterpart_type,
        evidence,
        count(distinct id_b) as n_counterparts
    from edges
    where evidence in (
        'E1_amplitude_event',     -- amplitude_id -> app users
        'E2_amplitude_event',     -- device_id    -> app users
        'E3_order_id',            -- solidgate customer -> app users
        'E6_amplitude_email',     -- email        -> app users
        'E9_mc_customer_id',      -- stripe customer -> app users
        'E12_mc_first_order_id',  -- stripe customer -> solidgate customers
        'E12_provider_payment_id', -- stripe customer -> solidgate customers (routed orders)
        'E7_checkout_order_id',   -- ga4 visitor  -> solidgate customers
        'E15_ga4_user_id',        -- ga4 visitor  -> site user ids
        'E9_mc_user_id',          -- stripe customer -> site user ids
        'E16_amplitude_site_user_id' -- site user id -> app users
    )
    group by 1, 2, 3, 4, 5

)

select
    product,
    id_type,
    id_value,
    counterpart_type,
    evidence,
    n_counterparts,
    if(n_counterparts > {{ var('identity_max_fanout', 5) }}, 'super_node', 'shared') as reason
from fanout
where n_counterparts > 1
