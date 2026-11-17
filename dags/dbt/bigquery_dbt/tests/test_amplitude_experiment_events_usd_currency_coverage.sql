{{ config(severity='warn') }}

-- currencies with a price but no rate in int_currency_conversion
select
    currency,
    count(*) as events_affected
from {{ ref('int_amplitude_experiment_events') }}
where timestamp >= timestamp_sub(current_timestamp(), interval 3 day)
    and amount_local is not null
    and currency is not null
    and amount_usd is null
group by currency
