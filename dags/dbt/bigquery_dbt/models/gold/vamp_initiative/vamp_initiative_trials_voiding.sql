{{ config(
    materialized='table',
    tags=["stripe", "gold", "daily"],
    partition_by={
        "field": "month",
        "data_type": "date",
        "granularity": "month"
    }
) }}

-- Initiative #2: Cancel Auth at Trial Start — impact on VAMP & revenue
-- (monthly). A trial auth (<=$1.50) is in scope if the same customer's first
-- real (>$1.50) successful charge lands within 114 hours after it — i.e. the
-- trial converted quickly enough that the auth itself wasn't needed. Voiding
-- those auths removes transactions/volume and forgoes their revenue, but
-- also avoids disputes from customers who go on to dispute a charge
-- elsewhere (estimated at a 0.25% propensity).

with ch as (

    select
        id,
        customer,
        status,
        captured,
        amount_usd,
        amount_captured_usd,
        created_ts,
        date(created_ts) as created_date
    from {{ ref('int_stripe_charges') }}
    where _elt_stripe_account_id = 'acct_1RoZfRALzAR0Hu3M'

),

trial_auths as (

    select * from ch where amount_usd <= 1.5

),

first_real_pay as (

    select customer, min(created_ts) as activated_ts
    from ch
    where status = 'succeeded' and amount_usd > 1.5
    group by customer

),

in_scope as (

    select t.*, f.activated_ts
    from trial_auths t
    inner join first_real_pay f using (customer)
    where f.activated_ts > t.created_ts
      and f.activated_ts < timestamp_add(t.created_ts, interval 114 hour)

),

disputers as (

    select distinct c.customer
    from {{ ref('int_stripe_disputes') }} d
    inner join ch c on c.id = d.charge

)

select
    date_trunc(s.created_date, month) as month,

    -- EFFECT 2: transactions removed
    countif(s.captured = false) as charges_voided,
    count(distinct if(s.captured = false, s.customer, null)) as customers_affected,

    -- EFFECT 3: revenue forgone
    round(sum(if(s.captured = false, s.amount_usd, 0)), 2) as revenue_forgone_usd,

    -- EFFECT 1: disputes avoided (assumed propensity 0.25%)
    countif(s.captured = false and s.customer in (select customer from disputers))
        as voided_charges_of_disputers,
    0.25 as dispute_propensity_pct,
    round(
        countif(s.captured = false and s.customer in (select customer from disputers)) * 0.0025,
        1
    ) as est_disputes_avoided,

    -- control check
    countif(s.captured = true) as in_scope_but_captured

from in_scope s
group by month
