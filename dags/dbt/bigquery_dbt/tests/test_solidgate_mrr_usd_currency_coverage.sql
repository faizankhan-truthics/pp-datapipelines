{{ config(severity='warn') }}

with currencies as (

    select distinct snapshot_date, currency
    from {{ ref('int_solidgate_mrr_local_daily') }}
    where upper(currency) != 'USD'

),

rates as (

    select distinct upper(currency) as currency
    from {{ ref('int_currency_conversion') }}

)

select
    c.currency,
    min(c.snapshot_date) as first_date_affected,
    max(c.snapshot_date) as last_date_affected,
    count(*)             as days_affected
from currencies c
left join rates r
    on upper(c.currency) = r.currency
where r.currency is null
group by c.currency
