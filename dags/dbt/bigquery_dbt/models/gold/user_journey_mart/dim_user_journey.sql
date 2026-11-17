{{ config(
    materialized='table',
    tags=["journey", "user_journey_mart", "gold", "daily"],
    cluster_by=['product', 'reached_stage']
) }}

-- One row per user of the journey mart: every known user at any stage -- anonymous Amplitude
-- visitor, signed-up app user, or payer -- with identity, first touch, stage timestamps and
-- current outcome (docs/plans/user_journey_mart.md, section 8.1).
--   identity      int_identity_resolution
--   attribution   int_user_touch_classified (KR2 rules, seeds/seed_kr2_channel_rules.csv):
--                 the journey's touch is the earliest eligible touch the rules assign a channel to;
--                 within touch_merge_minutes of it (the same visit seen by two tools) the most
--                 informative method wins. Eligible: on or before the anchor (signup, else first
--                 payment; plus kr2_anchor_grace_minutes of clock skew) and no more than
--                 kr2_lookback_days before it; a visitor without an anchor has no window. No such
--                 touch: the fallback_channel_group of the earliest skipped touch that names a
--                 source (is_fallback_attribution); none either: unattributed (J01). Both go on
--                 the review list
--   stages        fct_user_journey_events
--   outcome       subscriptions and MRR from user_subscription_mart.fct_subscription, lifetime
--                 revenue from int_customer_revenue -- a user with an active subscription on
--                 EITHER provider is an active customer (trials often start on Solidgate and
--                 continue on Stripe)
-- The first_touch_* columns describe the attributing touch; an unattributed journey shows its
-- earliest touch instead, for review. kr2_taxonomy_enabled (default true) can blank the taxonomy
-- columns.
-- History starts are derived from the data (no fixed dates): has_amplitude_history says whether
-- the journey started after the product's first Amplitude event; is_journey_start_truncated
-- marks payers whose first payment predates every visit source.
-- Grain: product x user_key.

with resolution as (

    select * from {{ ref('int_identity_resolution') }}

),

registry as (

    select product, dormancy_days
    from {{ ref('seed_product_registry') }}
    where is_in_scope

),

events as (

    select * from {{ ref('fct_user_journey_events') }}

),

-- ---------- identity ----------

users as (

    select
        product,
        user_key,
        -- min / ordered aggregates, not any_value: a user with several ids gets the same values on
        -- every run
        min(anchor_type)                                                        as anchor_type,
        -- the hub, not a backend-only alias of it
        coalesce(min(if(id_type = 'app_user_id' and link_path = 'hub', id_value, null)),
                 min(if(id_type = 'app_user_id', id_value, null)))              as app_user_id,
        min(if(id_type = 'email', id_value, null))                              as email_user_key,
        coalesce(
            string_agg(distinct if(id_type in ('solidgate_customer', 'stripe_customer'),
                                   concat(split(id_type, '_')[offset(0)], ':', link_path), null),
                       ', '
                       order by if(id_type in ('solidgate_customer', 'stripe_customer'),
                                   concat(split(id_type, '_')[offset(0)], ':', link_path), null)),
            min(if(id_type = 'amplitude_id', link_path, null)),
            min(if(id_type = 'email', link_path, null)),
            'hub'
        )                                                                       as link_path,
        case
            when countif(identity_confidence = 'high') > 0   then 'high'
            when countif(identity_confidence = 'medium') > 0 then 'medium'
            else 'none'
        end                                                                     as identity_confidence,
        countif(id_type = 'ga4_pseudo_id') > 0                                  as has_ga4_match,
        countif(id_type = 'amplitude_id') > 0                                   as has_amplitude_match,
        countif(id_type = 'anytrack_click_id') > 0                              as has_anytrack_match,
        countif(id_type in ('solidgate_customer', 'stripe_customer')) > 0       as has_payment_match,
        countif(id_type = 'device_id')                                          as n_devices
    from resolution
    group by 1, 2

),

-- ---------- stages ----------

stages as (

    select
        product,
        user_key,
        min(if(event_source in ('amplitude', 'ga4', 'anytrack'), event_ts, null)) as visitor_ts,
        min(if(stage = 'signup', event_ts, null))                               as signup_ts,
        array_agg(if(stage = 'signup', event_source, null) ignore nulls
                  order by event_ts limit 1)[safe_offset(0)]                    as signup_source,
        min(if(stage = 'activation', event_ts, null))                           as activation_ts,
        min(if(stage = 'trial', event_ts, null))                                as trial_ts,
        logical_or(event_name = 'free_trial_order')                             as has_free_trial_order,
        min(if(stage = 'payment_attempt', event_ts, null))                      as payment_attempt_ts,
        countif(stage = 'payment_attempt')                                      as n_payment_attempts,
        min(if(stage = 'customer', event_ts, null))                             as first_payment_ts,
        min(if(stage = 'renewal' or event_name = 'subscription_converted', event_ts, null)) as converted_ts,
        countif(stage = 'renewal')                                              as n_renewals,
        array_agg(if(stage = 'customer', event_source, null) ignore nulls
                  order by event_ts limit 1)[safe_offset(0)]                    as first_payment_provider,
        array_agg(if(stage in ('customer', 'renewal'), event_source, null) ignore nulls
                  order by event_ts desc limit 1)[safe_offset(0)]               as current_payment_provider,
        logical_or(event_source = 'solidgate' and stage in ('customer', 'renewal')) as paid_on_solidgate,
        logical_or(event_source = 'stripe' and stage in ('customer', 'renewal'))    as paid_on_stripe,
        max(if(stage in ('customer', 'renewal'), event_ts, null))               as last_payment_ts
    from events
    group by 1, 2

),

-- ---------- attribution (KR2) ----------

touches as (

    select
        c.*,
        -- the order touches are compared in: a date-only touch (payment) comes after the
        -- timestamped touches of its day, which carry the actual click
        if(c.is_touch_date_only,
           timestamp_sub(timestamp_add(c.touch_ts, interval 1 day), interval 1 microsecond),
           c.touch_ts)                                                  as order_ts,
        coalesce(st.signup_ts, st.first_payment_ts)                     as anchor_ts
    from {{ ref('int_user_touch_classified') }} c
    join stages st on st.product = c.product and st.user_key = c.user_key

),

-- E01: a touch after the anchor, or further back than the lookback, is not eligible
eligible as (

    select *
    from touches
    where decision != 'ignore'
      and touch_ts is not null
      and (anchor_ts is null
           or (touch_ts <= timestamp_add(anchor_ts, interval {{ var('kr2_anchor_grace_minutes', 10) }} minute)
               and touch_ts >= timestamp_sub(anchor_ts, interval {{ var('kr2_lookback_days', 30) }} day)))

),

winner_window as (

    select
        e.*,
        min(e.order_ts) over (partition by e.product, e.user_key)       as earliest_assigned_ts
    from eligible e
    where e.decision = 'assign'

),

winner as (

    select *
    from winner_window
    where order_ts <= timestamp_add(earliest_assigned_ts, interval {{ var('touch_merge_minutes', 30) }} minute)
    qualify row_number() over (
        partition by product, user_key order by method_rank, order_ts, touch_system, touch_id
    ) = 1

),

-- no assigned touch: the earliest skipped touch that still names a source gives its
-- fallback_channel_group, so a journey is unattributed only when no source is known at all
fallback as (

    select * replace (fallback_channel_group as channel_group)
    from eligible
    where decision = 'skip_flag'
      and fallback_channel_group is not null
    qualify row_number() over (
        partition by product, user_key order by order_ts, method_rank, touch_system, touch_id
    ) = 1

),

chosen as (

    select * except (earliest_assigned_ts), false as is_fallback
    from winner

    union all

    select f.*, true as is_fallback
    from fallback f
    left join winner w on w.product = f.product and w.user_key = f.user_key
    where w.user_key is null

),

-- an unattributed journey shows its earliest touch, so the review has the raw values
earliest_touch as (

    select *
    from touches
    qualify row_number() over (
        partition by product, user_key order by order_ts, method_rank, touch_system, touch_id
    ) = 1

),

-- skipped touches before the winner (all of them when there is none) explain the review flag
review as (

    select
        e.product,
        e.user_key,
        count(*)                                                        as n_eligible_touches,
        countif(e.decision = 'skip_flag')                               as n_skipped_touches,
        array_agg(
            if(e.decision = 'skip_flag' and (w.order_ts is null or e.order_ts <= w.order_ts),
               struct(e.rule_id, e.touch_system, e.touch_ts, e.review_reason as reason), null)
            ignore nulls order by e.order_ts limit 10
        )                                                               as review_reasons
    from eligible e
    left join winner w on w.product = e.product and w.user_key = e.user_key
    group by 1, 2

),

attribution as (

    select
        st.product,
        st.user_key,
        coalesce(w.touch_ts, t.touch_ts)                                as touch_ts,
        coalesce(w.touch_system, t.touch_system)                        as touch_system,
        if(w.user_key is not null, w.source, t.source)                  as source,
        if(w.user_key is not null, w.medium, t.medium)                  as medium,
        if(w.user_key is not null, w.campaign, t.campaign)              as campaign,
        if(w.user_key is not null, w.term, t.term)                      as term,
        if(w.user_key is not null, w.referrer_domain, t.referrer_domain) as referrer_domain,
        if(w.user_key is not null, w.landing_page, t.landing_page)      as landing_page,
        if(w.user_key is not null, w.ad_network, t.ad_network)          as ad_network,
        if(w.user_key is not null, w.click_id_type, t.click_id_type)    as click_id_type,
        if(w.user_key is not null, w.click_id, t.click_id)              as click_id,
        coalesce(w.channel_group, 'unattributed')                       as channel_group,
        coalesce(w.source_resolution_method, 'none')                    as source_resolution_method,
        coalesce(w.rule_id, 'J01')                                      as rule_id,
        coalesce(w.is_fallback, false)                                  as is_fallback,
        coalesce(st.signup_ts, st.first_payment_ts)                     as anchor_ts,
        case
            when st.signup_ts is not null        then 'signup'
            when st.first_payment_ts is not null then 'first_payment'
            else 'none'
        end                                                             as anchor_type,
        coalesce(r.n_eligible_touches, 0)                               as n_eligible_touches,
        coalesce(r.n_skipped_touches, 0)                                as n_skipped_touches,
        r.review_reasons
    from stages st
    left join chosen w         on w.product = st.product and w.user_key = st.user_key
    left join earliest_touch t on t.product = st.product and t.user_key = st.user_key
    left join review r         on r.product = st.product and r.user_key = st.user_key

),

-- last activity: any Amplitude or AnyTrack event of the user, not just the timeline's
activity as (

    select product, user_key, max(last_seen_ts) as last_seen_ts
    from (
        select
            e.product,
            coalesce(
                ra.user_key,
                if(e.app_user_id is not null, to_hex(md5(concat(e.product, '|app_user|', e.app_user_id))), null),
                r.user_key
            )                                                                   as user_key,
            max(e.event_ts)                                                     as last_seen_ts
        from {{ ref('int_amplitude_events') }} e
        left join resolution r
            on  r.product  = e.product
            and r.id_type  = 'amplitude_id'
            and r.id_value = e.amplitude_id
        left join resolution ra
            on  ra.product  = e.product
            and ra.id_type  = 'app_user_id'
            and ra.id_value = e.app_user_id
        group by 1, 2

        union all

        select a.product, r.user_key, max(a.event_ts)
        from {{ ref('int_anytrack_events') }} a
        join resolution r
            on  r.product  = a.product
            and r.id_type  = 'email'
            and r.id_value = a.email_user_key
        group by 1, 2
    )
    where user_key is not null
    group by 1, 2

),

-- ---------- outcome ----------

payer_ids as (

    select product, user_key, id_type, id_value
    from resolution
    where id_type in ('solidgate_customer', 'stripe_customer')

),

-- fct_subscription.mrr_usd is the MRR of the subscription's LAST snapshot, so a subscription
-- cancelled months ago still carries its old MRR. It is current only when that snapshot is the
-- source's latest one (the subscription is still in today's MRR).
latest_mrr_snapshot as (

    select source_system, max(mrr_as_of_date) as latest_mrr_as_of_date
    from {{ ref('fct_subscription') }}
    group by 1

),

subscriptions as (

    select
        p.product,
        p.user_key,
        count(*)                                                                as n_subscriptions,
        sum(if(s.mrr_usd > 0 and is_current, s.mrr_usd, 0))                     as current_mrr_usd,
        countif(s.mrr_usd > 0 and is_current) > 0                               as has_active_subscription,
        max(s.cancelled_date)                                                   as last_cancelled_date
    from (
        select s.*, s.mrr_as_of_date = l.latest_mrr_as_of_date as is_current
        from {{ ref('fct_subscription') }} s
        join latest_mrr_snapshot l using (source_system)
    ) s
    join payer_ids p
        on  p.id_type  = concat(s.source_system, '_customer')
        and p.id_value = concat(s.source_account, ':', s.source_customer_id)
    group by 1, 2

),

revenue as (

    select
        p.product,
        p.user_key,
        sum(v.lifetime_revenue_usd)                                             as ltv_usd
    from {{ ref('int_customer_revenue') }} v
    join payer_ids p
        on  p.id_type  = concat(v.source_system, '_customer')
        and p.id_value = concat(v.source_account, ':', v.source_customer_id)
    group by 1, 2

),

-- ---------- data-derived history starts ----------

amplitude_start as (

    select product, min(event_ts) as amplitude_start_ts
    from {{ ref('int_amplitude_events') }}
    group by 1

),

ga4_start as (

    select product, timestamp(min(ga4_date)) as ga4_start_ts
    from {{ ref('int_ga4_journey_events') }}
    group by 1

),

anytrack_start as (

    select product, min(event_ts) as anytrack_start_ts
    from {{ ref('int_anytrack_events') }}
    group by 1

),

history as (

    select r.product, a.amplitude_start_ts, g.ga4_start_ts, t.anytrack_start_ts
    from registry r
    left join amplitude_start a on a.product = r.product
    left join ga4_start g       on g.product = r.product
    left join anytrack_start t  on t.product = r.product

),

joined as (

    select
        u.*,
        ft.touch_ts                                                             as first_touch_ts,
        ft.touch_system                                                         as first_touch_system,
        ft.source                                                               as first_touch_source,
        ft.medium                                                               as first_touch_medium,
        ft.campaign                                                             as first_touch_campaign,
        ft.term                                                                 as first_touch_term,
        ft.referrer_domain                                                      as first_touch_referrer_domain,
        ft.landing_page                                                         as first_touch_landing_page,
        ft.ad_network,
        coalesce(ft.click_id_type, 'none')                                      as click_id_type,
        ft.click_id,
        ft.channel_group                                                        as attr_channel_group,
        ft.source_resolution_method                                             as attr_resolution_method,
        ft.rule_id                                                              as attr_rule_id,
        ft.is_fallback                                                          as attr_is_fallback,
        ft.anchor_ts                                                            as attr_anchor_ts,
        ft.anchor_type                                                          as attr_anchor_type,
        ft.n_eligible_touches,
        ft.n_skipped_touches,
        ft.review_reasons,
        st.* except (product, user_key),
        aa.last_seen_ts,
        sb.n_subscriptions,
        sb.current_mrr_usd,
        sb.has_active_subscription,
        sb.last_cancelled_date,
        rv.ltv_usd,
        h.amplitude_start_ts,
        h.ga4_start_ts,
        h.anytrack_start_ts,
        rg.dormancy_days
    from users u
    left join attribution ft        on ft.product = u.product and ft.user_key = u.user_key
    -- only users with a timeline: processor customers that never had a payment attempt (e.g. a
    -- Stripe customer created at checkout, never charged) stay in identity resolution and show
    -- up here once their first event arrives
    join stages st                  on st.product = u.product and st.user_key = u.user_key
    left join activity aa           on aa.product = u.product and aa.user_key = u.user_key
    left join subscriptions sb      on sb.product = u.product and sb.user_key = u.user_key
    left join revenue rv            on rv.product = u.product and rv.user_key = u.user_key
    left join history h             on h.product  = u.product
    left join registry rg           on rg.product = u.product

),

shaped as (

select
    -- identity
    user_key,
    product,
    anchor_type,
    app_user_id,
    email_user_key,
    link_path,
    identity_confidence,
    has_ga4_match,
    has_amplitude_match,
    has_anytrack_match,
    has_payment_match,
    n_devices,

    -- first touch
    date((select min(t) from unnest([first_touch_ts, visitor_ts, signup_ts, first_payment_ts]) as t))
                                                                                as journey_start_date,
    first_touch_ts,
    first_touch_system,
    first_touch_source,
    first_touch_medium,
    first_touch_campaign,
    first_touch_term,
    first_touch_referrer_domain,
    first_touch_landing_page,
    ad_network,
    click_id_type,
    click_id,

    -- attribution (KR2 spec)
    {% if var('kr2_taxonomy_enabled', true) %}
    attr_channel_group                                                          as channel_group,
    attr_resolution_method                                                      as source_resolution_method,
    attr_channel_group = 'unattributed'                                         as is_unattributed,
    attr_rule_id                                                                as attribution_rule_id,
    attr_is_fallback                                                            as is_fallback_attribution,
    -- skipped touches after the attributing one do not matter: review_reasons holds the ones before
    attr_channel_group = 'unattributed' or attr_is_fallback
        or coalesce(array_length(review_reasons), 0) > 0                        as needs_attribution_review,
    review_reasons                                                              as attribution_review_reasons,
    {% else %}
    cast(null as string)                                                        as channel_group,
    cast(null as string)                                                        as source_resolution_method,
    cast(null as bool)                                                          as is_unattributed,
    cast(null as string)                                                        as attribution_rule_id,
    cast(null as bool)                                                          as is_fallback_attribution,
    cast(null as bool)                                                          as needs_attribution_review,
    cast(null as array<struct<rule_id string, touch_system string, touch_ts timestamp, reason string>>)
                                                                                as attribution_review_reasons,
    {% endif %}
    attr_anchor_ts                                                              as attribution_anchor_ts,
    attr_anchor_type                                                            as attribution_anchor_type,
    n_eligible_touches,
    n_skipped_touches,
    -- the journey's start was not observed: the first payment predates every visit source, or
    -- predates the user's first recorded visit (a customer from before tracking who came back later)
    coalesce(first_payment_ts is not null
             and (
                 -- the earliest start among the product's visit sources (a source it lacks is no
                 -- bound); no visit source at all: nothing before the payment was observed
                 (visitor_ts is null
                  and first_payment_ts < coalesce(
                      (select min(t) from unnest([amplitude_start_ts, ga4_start_ts, anytrack_start_ts]) as t),
                      timestamp_add(first_payment_ts, interval 1 second)))
                 or date(first_payment_ts) < date(visitor_ts)
             ), false)                                                          as is_journey_start_truncated,
    coalesce(coalesce(first_payment_ts, visitor_ts, signup_ts) >= amplitude_start_ts, false)
                                                                                as has_amplitude_history,

    -- stages
    visitor_ts,
    signup_ts,
    signup_source,
    activation_ts,
    trial_ts,
    case
        when has_free_trial_order then 'free'
        when trial_ts is not null then 'paid'
    end                                                                         as trial_type,
    payment_attempt_ts,
    coalesce(n_payment_attempts, 0)                                             as n_payment_attempts,
    first_payment_ts,
    -- a conversion needs a first payment: Amplitude subscription_converted can fire for users
    -- whose payment never reached the mart, which would mark non-customers as converted
    if(first_payment_ts is not null, converted_ts, null)                        as converted_ts,
    case
        when first_payment_ts is not null   then 'customer'
        -- the processor saw a real attempt (declined, voided, never collected), but no money came in
        when payment_attempt_ts is not null then 'payment_attempted'
        when activation_ts is not null      then 'activation'
        when signup_ts is not null or anchor_type = 'app_user' then 'signup'
        else 'visitor'
    end                                                                         as reached_stage,

    -- outcome
    first_payment_ts is not null                                                as is_customer,
    first_payment_ts is not null and converted_ts is not null                   as is_converted,
    case
        when paid_on_solidgate and paid_on_stripe then 'both'
        when paid_on_solidgate then 'solidgate'
        when paid_on_stripe    then 'stripe'
    end                                                                         as payment_provider,
    first_payment_provider,
    current_payment_provider,
    coalesce(n_renewals, 0)                                                     as n_renewals,
    case
        when first_payment_ts is not null then
            case
                when has_active_subscription then 'active_customer'
                -- one-off buyers with no subscription: active while they keep buying
                when coalesce(n_subscriptions, 0) = 0
                     and last_payment_ts >= timestamp_sub(current_timestamp(), interval dormancy_days day)
                    then 'active_customer'
                else 'churned_customer'
            end
        when anchor_type = 'app_user' or signup_ts is not null then
            if(last_seen_ts >= timestamp_sub(current_timestamp(), interval dormancy_days day),
               'registered_active', 'registered_dormant')
        else
            if(last_seen_ts >= timestamp_sub(current_timestamp(), interval dormancy_days day),
               'anonymous_active', 'dormant_visitor')
    end                                                                         as current_state,
    last_cancelled_date                                                         as _last_cancelled_date,
    last_payment_ts                                                             as _last_payment_ts,
    last_seen_ts,
    -- MRR is recurring revenue of paying users: a subscription whose first payment is not
    -- collected yet (e.g. a trial inside Stripe's capture delay) counts once it is
    if(first_payment_ts is not null, round(coalesce(current_mrr_usd, 0), 2), 0) as current_mrr_usd,
    round(coalesce(ltv_usd, 0), 2)                                              as ltv_usd,

    -- velocity
    date_diff(date(signup_ts), date(visitor_ts), day)                           as days_visit_to_signup,
    date_diff(date(activation_ts), date(signup_ts), day)                        as days_signup_to_activation,
    -- null when the first payment predates the first recorded visit (journey start not observed)
    if(date(first_payment_ts) >= date(visitor_ts),
       date_diff(date(first_payment_ts), date(visitor_ts), day), null)          as days_visit_to_customer,

    -- audit
    current_timestamp()                                                         as _built_at
from joined

)

select
    * except (_last_cancelled_date, _last_payment_ts),
    if(current_state = 'churned_customer',
       coalesce(_last_cancelled_date, date(_last_payment_ts)), null)            as churn_date
from shaped
