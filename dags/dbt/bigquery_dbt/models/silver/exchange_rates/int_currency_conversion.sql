{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['exchange_date', 'currency'],
    on_schema_change='append_new_columns',
    tags=["exchange_date", "silver", "daily"],
    partition_by={
        "field": "exchange_date",
        "data_type": "date"
    }
) }}

--currencies in data
with currencies as (
    select
        distinct currency as currency
    --recent currencies
    {% if is_incremental() %}
    from {{ source('bronze_fx', 'daily_fx') }}
     union distinct
    select
        distinct base_currency as currency
    from {{ ref('history_fx') }}
    where date(date) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    --hardcoded currency
    union distinct
    select 'BGN'
    --all currencies
    {% else %}
    from {{ source('bronze_fx', 'daily_fx') }}
    union distinct
    select
        distinct base_currency as currency
    from {{ ref('history_fx') }}
    --hardcoded currency
    union distinct
    select 'BGN'
    {% endif %}
),

--generate all possible date<>currency combination
dates as (
    select
        exchange_date
    -- recent days dates
    {% if is_incremental() %}
        from unnest(
        generate_date_array(
            DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY),
            current_date()
        )
    ) as exchange_date
    --all dates in data
    {% else %}
        from unnest(
            generate_date_array(
                (select min(date(date)) from {{ ref('history_fx') }}),
                current_date()
            )
        ) as exchange_date
    {% endif %}
),

date_currency as (
    select
        d.exchange_date,
        c.currency
    from dates d
    cross join currencies c
),

daily as (
    select
        date(date) as exchange_date,
        currency,
        SAFE_CAST(close AS FLOAT64) AS rate,
        'daily' as source
    from {{ source('bronze_fx', 'daily_fx') }}
    {% if is_incremental() %}
        where date(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    {% endif %}

),

combined_rates as (
    select exchange_date, currency, rate, source
    from (
        --get yesterdays data for fill current run
        select
            date(date) as exchange_date,
            base_currency as currency,
            SAFE_CAST(close AS FLOAT64) AS rate,
            'history' as source
        from {{ ref('history_fx') }}
        {% if is_incremental() %}
            where date(date) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        {% endif %}
        union all
        select * from daily
    )
    qualify row_number() over (
        partition by exchange_date, currency
        order by
        case
            when source = 'daily' and rate is not null then 1
            when source = 'history' and rate is not null then 2
            else 3
        end
    ) = 1
),

filled as (
    select
        dc.exchange_date,
        dc.currency,
        case when dc.currency = 'BGN' then
            --Combined for BGN: amount_usd = (amount_bgn / 1.95583) * eur_usd_rate_on_invoice_date
            safe_divide(
                coalesce(
                    c_bgn.rate,
                    last_value(c_bgn.rate ignore nulls) over (
                        partition by dc.currency
                        order by dc.exchange_date
                        rows between unbounded preceding and 1 preceding
                    )
                )
            , 1.95583)
            else
                coalesce(
                    c.rate,
                    last_value(c.rate ignore nulls) over (
                        partition by dc.currency
                        order by dc.exchange_date
                        rows between unbounded preceding and 1 preceding
                    )
                )
        end as rate,
        case when c.rate is null then 'fill'
              else c.source
        end as source
    from date_currency dc
    left join combined_rates c
        on dc.exchange_date = c.exchange_date
        and dc.currency = c.currency
    --hardcode BGN rate
    left join combined_rates c_bgn
        on dc.currency = 'BGN'
        and c_bgn.currency = 'EUR'
        and dc.exchange_date = c_bgn.exchange_date

)

select
    exchange_date,
    currency,
    rate,
    source
from filled