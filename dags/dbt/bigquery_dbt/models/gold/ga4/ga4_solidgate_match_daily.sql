{{ config(
    tags=["ga4", "gold", "daily"],
) }}

-- How many payments were seen by both GA4 and Solidgate, per product per day.
-- Grain: match_date x _elt_account_name. Totals across products are a group by here,
-- not a row in this table — mixing grains invites double counting.

with ga4 as (

    -- int_ga4_transactions now also covers Stripe-backed properties (their
    -- _elt_account_name is null, since that column only means something for Solidgate) --
    -- this mart is Solidgate-specific, so exclude them rather than let them collapse into a
    -- spurious "null account" group below.
    select ga4_date, _elt_account_name, transaction_id
    from {{ ref('int_ga4_transactions') }}
    where _elt_account_name is not null

),

solidgate as (

    select sg_date, _elt_account_name, transaction_id
    from {{ ref('int_solidgate_transactions') }}

),

stitched as (

    select
        coalesce(s.sg_date, g.ga4_date)                   as match_date,
        coalesce(s._elt_account_name, g._elt_account_name) as _elt_account_name,
        s.transaction_id                                  as sg_txn_id,
        g.transaction_id                                  as ga4_txn_id
    from solidgate s
    full outer join ga4 g
        on  s.transaction_id    = g.transaction_id
        and s.sg_date           = g.ga4_date
        -- match within a product: one product's ids must never meet another's events
        and s._elt_account_name = g._elt_account_name

)

select
    match_date,
    _elt_account_name,
    countif(sg_txn_id is not null)                              as sg_txn_cnt,
    countif(ga4_txn_id is not null)                             as ga4_txn_cnt,
    countif(sg_txn_id is not null and ga4_txn_id is not null)   as ga4_sg_matched,
    countif(sg_txn_id is not null and ga4_txn_id is null)       as sg_not_matched,
    countif(sg_txn_id is null and ga4_txn_id is not null)       as ga4_not_matched,
    round(safe_divide(
        countif(sg_txn_id is not null and ga4_txn_id is not null),
        countif(sg_txn_id is not null)
    ) * 100, 2) as sg_match_pct,
    round(safe_divide(
        countif(sg_txn_id is not null and ga4_txn_id is not null),
        countif(ga4_txn_id is not null)
    ) * 100, 2) as ga4_match_pct
from stitched
group by match_date, _elt_account_name
