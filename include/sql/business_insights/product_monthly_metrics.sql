-- Month-by-month growth metrics for ONE product, defined as a set of PSP accounts
-- (@solidgate_accounts / @stripe_accounts query parameters). Mirrors the Executive
-- "Growth Rate Product Dashboard" logic: MRR waterfall and customer waterfall from
-- *_mrr_waterfall_daily, month-end MRR / subscriptions from mrr_arr_daily.
--
-- Unlike the dashboard's "Net MRR Change" column, net_mrr_change here is ending - beginning
-- and movements_mrr is the signed sum of the five movements (churn/contraction are already
-- negative in the marts). The gap between them is surfaced, not hidden.
-- The current month is partial; `is_partial` / `data_through` flag it.
with wf as (

    select 'solidgate' as provider, snapshot_date, _elt_account_name as account_name,
           new_business_mrr_usd, reactivation_mrr_usd, expansion_mrr_usd, contraction_mrr_usd, churn_mrr_usd,
           new_business_customers, reactivation_customers, expansion_customers, contraction_customers,
           churn_customers, active_customers
    from `{{ params.bq_project }}.{{ params.gold_dataset }}.solidgate_mrr_waterfall_daily`
    where _elt_account_name in unnest(@solidgate_accounts)

    union all

    select 'stripe', snapshot_date, account_name,
           new_business_mrr_usd, reactivation_mrr_usd, expansion_mrr_usd, contraction_mrr_usd, churn_mrr_usd,
           new_business_customers, reactivation_customers, expansion_customers, contraction_customers,
           churn_customers, active_customers
    from `{{ params.bq_project }}.{{ params.gold_dataset }}.stripe_mrr_waterfall_daily`
    where account_name in unnest(@stripe_accounts)

),

wf_month as (

    select
        date_trunc(snapshot_date, month)  as month_start,
        sum(new_business_mrr_usd)         as new_business_mrr,
        sum(reactivation_mrr_usd)         as reactivation_mrr,
        sum(expansion_mrr_usd)            as expansion_mrr,
        sum(contraction_mrr_usd)          as contraction_mrr,
        sum(churn_mrr_usd)                as churn_mrr,
        sum(new_business_customers)       as new_customers,
        sum(reactivation_customers)       as reactivated_customers,
        sum(expansion_customers)          as expansion_customers,
        sum(contraction_customers)        as contraction_customers,
        sum(churn_customers)              as churned_customers
    from wf
    group by 1

),

customers_end as (

    select month_start, sum(active_customers) as ending_subscribers
    from (
        select date_trunc(snapshot_date, month) as month_start, active_customers
        from wf
        qualify row_number() over (
            partition by date_trunc(snapshot_date, month), provider, account_name
            order by snapshot_date desc
        ) = 1
    )
    group by 1

),

mrr_end as (

    select
        month_start,
        max(snapshot_date)           as data_through,
        sum(mrr_usd)                 as ending_mrr,
        sum(active_subscriptions)    as ending_subscriptions
    from (
        select date_trunc(snapshot_date, month) as month_start, snapshot_date, mrr_usd, active_subscriptions
        from `{{ params.bq_project }}.{{ params.gold_dataset }}.mrr_arr_daily`
        where (payment_source = 'solidgate' and account_name in unnest(@solidgate_accounts))
           or (payment_source = 'stripe'    and account_name in unnest(@stripe_accounts))
        qualify row_number() over (
            partition by date_trunc(snapshot_date, month), payment_source, account_name
            order by snapshot_date desc
        ) = 1
    )
    group by 1

),

joined as (

    select
        e.month_start,
        e.data_through,
        e.month_start = date_trunc(current_date(), month)         as is_partial,
        lag(e.ending_mrr) over w                                  as beginning_mrr,
        coalesce(f.new_business_mrr, 0)                           as new_business_mrr,
        coalesce(f.reactivation_mrr, 0)                           as reactivation_mrr,
        coalesce(f.expansion_mrr, 0)                              as expansion_mrr,
        coalesce(f.contraction_mrr, 0)                            as contraction_mrr,
        coalesce(f.churn_mrr, 0)                                  as churn_mrr,
        e.ending_mrr,
        lag(c.ending_subscribers) over w                          as beginning_subscribers,
        coalesce(f.new_customers, 0)                              as new_subscribers,
        coalesce(f.reactivated_customers, 0)                      as reactivated_subscribers,
        coalesce(f.expansion_customers, 0)                        as expansion_customers,
        coalesce(f.contraction_customers, 0)                      as contraction_customers,
        coalesce(f.churned_customers, 0)                          as churned_subscribers,
        c.ending_subscribers,
        lag(e.ending_subscriptions) over w                        as beginning_subscriptions,
        e.ending_subscriptions
    from mrr_end e
    left join wf_month f using (month_start)
    left join customers_end c using (month_start)
    window w as (order by e.month_start)

)

select
    *,
    ending_mrr - beginning_mrr as net_mrr_change,
    new_business_mrr + reactivation_mrr + expansion_mrr + contraction_mrr + churn_mrr as movements_mrr,
    safe_divide(ending_mrr - beginning_mrr, beginning_mrr)                               as mrr_growth,
    safe_divide(ending_subscribers - beginning_subscribers, beginning_subscribers)       as subscriber_growth,
    safe_divide(ending_subscriptions - beginning_subscriptions, beginning_subscriptions) as subscription_growth
from joined
where month_start >= date_sub(date_trunc(current_date(), month), interval 6 month)
order by month_start
