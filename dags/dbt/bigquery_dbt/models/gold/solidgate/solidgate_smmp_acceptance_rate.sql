{{ config(
    materialized='table',
    tags=["solidgate", "gold", "daily"],
    cluster_by=["account_name", "psp_name", "mid"]
) }}

-- Mastercard SMMP acceptance-rate monitor: rolling 72h auth rate per account / PSP / MID,
-- compared with the 72h window before it. Rows are flagged RED when a MID with at least
-- 25 attempts drops 50+ points against the prior window, or sits below a 30% auth rate.
-- Accepted statuses match solidgate_acceptance_decline_weekly.

with daily as (
    select
        _elt_account_name as account_name,
        psp_name,
        merchant_id as mid,
        created_date as dt,
        count(*) as cnt,
        countif(status in ('auth_ok', 'settle_ok', 'void_ok', 'refunded')) as auth
    from {{ ref('int_solidgate_card_orders') }}
    where lower(transaction_card_brand) = 'mastercard'
        and psp_name <> 'Fake'
        and created_date >= date_sub(current_date(), interval 20 day)
    group by 1, 2, 3, 4
),

-- one row per MID per day, so the ROWS windows below always span calendar days
spine as (
    select
        m.account_name,
        m.psp_name,
        m.mid,
        d as dt
    from (select distinct account_name, psp_name, mid from daily) m
    cross join unnest(generate_date_array(
        (select min(dt) from daily),
        (select max(dt) from daily),
        interval 1 day
    )) as d
),

filled as (
    select
        s.account_name,
        s.psp_name,
        s.mid,
        s.dt,
        coalesce(d.cnt, 0) as cnt,
        coalesce(d.auth, 0) as auth
    from spine s
    left join daily d
        on d.account_name = s.account_name
        and d.psp_name is not distinct from s.psp_name
        and d.mid is not distinct from s.mid
        and d.dt = s.dt
),

windowed as (
    select
        account_name,
        psp_name,
        mid,
        dt,
        sum(cnt) over (partition by account_name, psp_name, mid order by dt
                       rows between 2 preceding and current row) as cnt_72h,
        sum(auth) over (partition by account_name, psp_name, mid order by dt
                        rows between 2 preceding and current row) as auth_72h,
        sum(cnt) over (partition by account_name, psp_name, mid order by dt
                       rows between 5 preceding and 3 preceding) as cnt_prev72h,
        sum(auth) over (partition by account_name, psp_name, mid order by dt
                        rows between 5 preceding and 3 preceding) as auth_prev72h
    from filled
),

rates as (
    select
        *,
        100 * safe_divide(auth_72h, cnt_72h) as auth_rate,
        100 * safe_divide(auth_prev72h, cnt_prev72h) as auth_rate_prev
    from windowed
    where cnt_72h >= 25
        and dt >= date_sub(current_date(), interval 10 day)
)

select
    account_name,
    psp_name,
    mid,
    dt as date,

    -- current 72h window
    date_sub(dt, interval 2 day) as window_from,
    dt as window_to,
    cnt_72h as total_cnt_72h,
    round(auth_rate, 1) as auth_rate_pct,

    -- prior 72h window
    date_sub(dt, interval 5 day) as prev_window_from,
    date_sub(dt, interval 3 day) as prev_window_to,
    cnt_prev72h as total_cnt_prev_72h,
    round(auth_rate_prev, 1) as auth_rate_prev_72h_pct,

    round(auth_rate_prev - auth_rate, 1) as drop_points,
    case
        when (cnt_prev72h >= 25 and auth_rate_prev - auth_rate >= 50)
            or auth_rate < 30
            then 'RED'
        else ''
    end as flag
from rates
