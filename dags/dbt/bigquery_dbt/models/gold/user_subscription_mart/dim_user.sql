{{ config(
    tags=["user_subscription_mart", "gold", "daily"],
) }}

-- One row per resolved identity (see int_identity_user_map for match logic).
-- Zendesk is joined in as enrichment only (ticket volume), not as an identity
-- source -- it has no subscription concept of its own, so it never participates
-- in int_identity_user_map. Matched the same way int_zendesk_refund_tickets does:
-- lower-cased requester email, no dedicated Zendesk "user" model exists yet.
-- Acquisition is first-touch across source records: the attribution of whichever of this
-- person's customer records started paying first (see int_customer_attribution). LTV
-- (int_customer_revenue) and current MRR are summed across all of them.
-- Attribution population is every customer that ever carried MRR, attributed or not.
-- Grain: user_key.

with identity_map as (

    select * from {{ ref('int_identity_user_map') }}
    where user_key is not null

),

solidgate_first_revenue as (

    select distinct
        customer_account_id as source_customer_id,
        _elt_account_name   as source_account,
        first_revenue_date
    from {{ ref('int_solidgate_mrr_customer_daily') }}

),

stripe_first_revenue as (

    select distinct
        customer                 as source_customer_id,
        _elt_stripe_account_id   as source_account,
        first_revenue_date
    from {{ ref('int_stripe_mrr_customer_daily') }}

),

enriched as (

    select
        im.user_key,
        im.email,
        im.source_system,
        im.source_account,
        im.source_customer_id,
        case im.source_system
            when 'solidgate' then sfr.first_revenue_date
            when 'stripe'    then stfr.first_revenue_date
        end as first_revenue_date
    from identity_map im
    left join solidgate_first_revenue sfr
        on  im.source_system     = 'solidgate'
        and im.source_customer_id = sfr.source_customer_id
        and im.source_account     = sfr.source_account
    left join stripe_first_revenue stfr
        on  im.source_system     = 'stripe'
        and im.source_customer_id = stfr.source_customer_id
        and im.source_account     = stfr.source_account

),

user_base as (

    select
        user_key,
        any_value(email) as email,
        logical_or(source_system = 'solidgate') as is_solidgate_customer,
        logical_or(source_system = 'stripe')    as is_stripe_customer,
        array_agg(distinct source_account ignore nulls) as source_accounts,
        -- distinct source rows behind this user_key. 1 is the expected case; higher
        -- means either the same person paying through several accounts/products, or
        -- (harder to tell apart from here) a shared email across different people.
        count(distinct concat(source_system, '|', source_account, '|', source_customer_id))
            as source_customer_records,
        min(first_revenue_date) as first_revenue_date
    from enriched
    group by user_key

),

-- ============ ATTRIBUTION + VALUE, per source customer record ============

mrr_daily as (

    select
        'solidgate'         as source_system,
        _elt_account_name   as source_account,
        customer_account_id as source_customer_id,
        snapshot_date,
        mrr_usd,
        first_revenue_date
    from {{ ref('int_solidgate_mrr_customer_daily') }}

    union all

    select
        'stripe'                as source_system,
        _elt_stripe_account_id  as source_account,
        customer                as source_customer_id,
        snapshot_date,
        mrr_usd,
        first_revenue_date
    from {{ ref('int_stripe_mrr_customer_daily') }}

),

-- the latest date each source has MRR for. a customer with no row on that date has no MRR
-- now, whatever they were worth on their own last snapshot.
source_latest as (

    select source_system, max(snapshot_date) as latest_snapshot_date
    from mrr_daily
    group by source_system

),

customer_latest as (

    select * except (rn)
    from (
        select
            *,
            row_number() over (
                partition by source_system, source_account, source_customer_id
                order by snapshot_date desc
            ) as rn
        from mrr_daily
    )
    where rn = 1

),

campaign_names as (

    select cast(campaign_id as string) as campaign_id, campaign_name
    from {{ source('bronze_google_ads', 'ads_Campaign_2747379648') }}
    qualify row_number() over (partition by campaign_id order by _data_date desc) = 1

),

customers as (

    select
        a.user_key,
        a.source_system,
        a.source_account,
        a.source_customer_id,
        a.attribution_status,
        a.source,
        a.medium,
        a.campaign,
        a.campaign_id,
        cn.campaign_name,
        a.gclid,
        a.attributed_from_date,
        cl.first_revenue_date,
        if(cl.snapshot_date = sl.latest_snapshot_date, cl.mrr_usd, 0) as current_mrr_usd,
        coalesce(r.lifetime_gross_revenue_usd, 0) as lifetime_gross_revenue_usd,
        coalesce(r.lifetime_refunded_usd, 0)      as lifetime_refunded_usd,
        coalesce(r.lifetime_revenue_usd, 0)       as lifetime_revenue_usd
    from {{ ref('int_customer_attribution') }} a
    left join customer_latest cl
        on  a.source_system      = cl.source_system
        and a.source_account     = cl.source_account
        and a.source_customer_id = cl.source_customer_id
    left join source_latest sl
        on a.source_system = sl.source_system
    left join {{ ref('int_customer_revenue') }} r
        on  a.source_system      = r.source_system
        and a.source_account     = r.source_account
        and a.source_customer_id = r.source_customer_id
    left join campaign_names cn
        on a.campaign_id = cn.campaign_id
    where a.user_key is not null

),

-- earliest-paying record wins; on a tie prefer one we could actually attribute
first_touch as (

    select
        user_key,
        source_system as acquisition_source_system,
        source_account as acquisition_source_account,
        attribution_status,
        source,
        medium,
        campaign,
        campaign_id,
        campaign_name,
        gclid,
        attributed_from_date
    from customers
    qualify row_number() over (
        partition by user_key
        order by
            first_revenue_date nulls last,
            attribution_status = 'attributed' desc,
            source_system,
            source_account,
            source_customer_id
    ) = 1

),

customer_value as (

    select
        user_key,
        countif(attribution_status = 'attributed') as attributed_source_customer_records,
        sum(current_mrr_usd)                       as current_mrr_usd,
        sum(lifetime_gross_revenue_usd)            as lifetime_gross_revenue_usd,
        sum(lifetime_refunded_usd)                 as lifetime_refunded_usd,
        sum(lifetime_revenue_usd)                  as lifetime_revenue_usd
    from customers
    group by user_key

),

zendesk_tickets_by_email as (

    select
        lower(u.email) as email,
        count(distinct t.id) as support_ticket_count
    from {{ source('bronze_zendesk', 'zendesk_tickets') }} t
    join {{ source('bronze_zendesk', 'zendesk_users') }} u on u.id = t.requester_id
    where u.email is not null
    group by lower(u.email)

),

refund_tickets_by_email as (

    select
        email,
        count(distinct ticket_id) as refund_ticket_count
    from {{ ref('int_zendesk_refund_tickets') }}
    group by email

)

select
    ub.user_key,
    ub.email,
    ub.is_solidgate_customer,
    ub.is_stripe_customer,
    ub.source_accounts,
    ub.source_customer_records,
    ub.first_revenue_date,
    -- users with no customer record in the attribution population (never carried MRR) get
    -- no attribution row; they are reported as unattributed rather than left null
    coalesce(ft.attribution_status, 'unattributed') as attribution_status,
    ft.acquisition_source_system,
    ft.acquisition_source_account,
    coalesce(ft.source,   'unattributed')          as source,
    coalesce(ft.medium,   'unattributed')          as medium,
    coalesce(ft.campaign, 'unattributed')          as campaign,
    ft.campaign_id,
    ft.campaign_name,
    ft.gclid,
    ft.attributed_from_date,
    coalesce(cv.attributed_source_customer_records, 0) as attributed_source_customer_records,
    coalesce(cv.current_mrr_usd, 0)                    as current_mrr_usd,
    coalesce(cv.lifetime_gross_revenue_usd, 0)         as lifetime_gross_revenue_usd,
    coalesce(cv.lifetime_refunded_usd, 0)              as lifetime_refunded_usd,
    coalesce(cv.lifetime_revenue_usd, 0)               as lifetime_revenue_usd,
    coalesce(zt.support_ticket_count, 0) as support_ticket_count,
    coalesce(rt.refund_ticket_count, 0)  as refund_ticket_count
from user_base ub
left join first_touch ft on ft.user_key = ub.user_key
left join customer_value cv on cv.user_key = ub.user_key
left join zendesk_tickets_by_email zt on zt.email = ub.email
left join refund_tickets_by_email rt on rt.email = ub.email
