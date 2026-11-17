{{ config(
    materialized='table',
    tags=["amplitude", "ab_test", "gold", "daily"],
    cluster_by=['product', 'experiment_flag_key', 'ab_user_id']
) }}

-- One row per user per experiment flag, for the A/B test results dashboard, for every product.
-- Built from silver.int_amplitude_events and rebuilt in full on every run. Only events from
-- ab_test_min_date (default 2026-10-01) are read; set it before the earliest experiment you need.
-- Every user with an [Experiment] Exposure event is in the table, with or without an app account.
-- ab_user_id is the person: the user_key of int_identity_resolution, resolved from the event's app
-- user id or, for events logged before login, its Amplitude id. That makes one person of a web and a
-- backend app id (Public Registry, IQ Center), of two devices, and of an exposure logged before
-- login and the trial after it. An id the model does not resolve falls back to the raw app_user_id or
-- amplitude_id. user_key moves when an identity resolves further, so do not store ab_user_id
-- outside dbt; the table is rebuilt in full on every run.
-- Users exposed to more than one variant of a flag are kept under the variant of their first
-- exposure and flagged is_multi_variant, so a dashboard can filter them out or keep them.
-- Only events at or after the user's first exposure count, and conversion / cancel only at or after
-- the user's first trial, so nothing from before the user entered the experiment or the trial is
-- credited to a variant.
-- A conversion counts only for users with a first payment in dim_user_journey (same user_key).
-- converted_7d: the first conversion falls within ab_test_conversion_window_days (default 7) of the
-- first trial. It is only meaningful where first_trial_ts is not null, and the conversion rate
-- should be read over is_mature users only (trial at least ab_test_maturity_days, default 8, ago),
-- otherwise recent trials count as not converted.
-- Product-specific metrics live in extension models with the same key, e.g. fct_ab_test_users_opengen.
-- Grain: product x experiment_flag_key x ab_user_id.

with ev_raw as (

    select
        product,
        event_type,
        event_ts,
        amplitude_id,
        app_user_id,
        experiment_flag_key,
        experiment_variant,
        landing_tool
    from {{ ref('int_amplitude_events') }}
    where event_date >= date('{{ var("ab_test_min_date", "2026-10-01") }}')
      and event_type in (
          '[Experiment] Exposure', 'trial_started', 'subscription_converted', 'subscription_cancel',
          'trial_canceled'
      )

),

resolution as (

    select product, id_type, id_value, user_key
    from {{ ref('int_identity_resolution') }}
    where id_type in ('app_user_id', 'amplitude_id')

),

ev as (

    select
        r.product,
        r.event_type,
        r.event_ts,
        r.amplitude_id,
        r.app_user_id,
        coalesce(ra.user_key, rb.user_key, r.app_user_id, r.amplitude_id) as ab_user_id,
        r.experiment_flag_key,
        r.experiment_variant,
        r.landing_tool
    from ev_raw as r
    left join resolution as ra
        on ra.product = r.product
        and ra.id_type = 'app_user_id'
        and ra.id_value = r.app_user_id
    left join resolution as rb
        on rb.product = r.product
        and rb.id_type = 'amplitude_id'
        and rb.id_value = r.amplitude_id

),

exposures as (

    select
        product,
        experiment_flag_key,
        ab_user_id,
        min(app_user_id) as app_user_id,
        min(amplitude_id) as amplitude_id,
        min_by(experiment_variant, event_ts) as variant,
        count(distinct experiment_variant) > 1 as is_multi_variant,
        min(event_ts) as first_exposure_ts,
        array_agg(landing_tool ignore nulls order by event_ts limit 1)[safe_offset(0)] as landing_tool
    from ev
    where event_type = '[Experiment] Exposure'
      and experiment_flag_key is not null
      and experiment_variant is not null
    group by product, experiment_flag_key, ab_user_id

),

-- first trial at or after the first exposure
trials as (

    select
        x.product,
        x.experiment_flag_key,
        x.ab_user_id,
        min(e.event_ts) as first_trial_ts
    from exposures as x
    inner join ev as e
        on e.product = x.product
        and e.ab_user_id = x.ab_user_id
        and e.event_type = 'trial_started'
        and e.event_ts >= x.first_exposure_ts
    group by x.product, x.experiment_flag_key, x.ab_user_id

),

-- conversion and cancel are only looked for at or after the user's first trial, so an earlier
-- one (from before the trial) never hides the trial's own
after_exposure as (

    select
        t.product,
        t.experiment_flag_key,
        t.ab_user_id,
        t.first_trial_ts,
        min(if(e.event_type = 'subscription_converted', e.event_ts, null)) as first_conversion_ts,
        min(
            if(e.event_type in ('subscription_cancel', 'trial_canceled'), e.event_ts, null)
        ) as first_cancel_ts
    from trials as t
    left join ev as e
        on e.product = t.product
        and e.ab_user_id = t.ab_user_id
        and e.event_type in ('subscription_converted', 'subscription_cancel', 'trial_canceled')
        and e.event_ts >= t.first_trial_ts
    group by t.product, t.experiment_flag_key, t.ab_user_id, t.first_trial_ts

),

-- same rule as dim_user_journey: a conversion only counts for a user with a real first payment,
-- Amplitude subscription_converted alone can fire for users who never paid
journey as (

    select product, user_key, app_user_id, first_payment_ts
    from {{ ref('dim_user_journey') }}

)

select
    x.product,
    x.experiment_flag_key,
    x.ab_user_id,
    coalesce(j.app_user_id, x.app_user_id) as app_user_id,
    x.amplitude_id,
    x.variant,
    x.is_multi_variant,
    x.landing_tool,
    x.first_exposure_ts,
    a.first_trial_ts,
    if(j.first_payment_ts is not null, a.first_conversion_ts, null) as first_conversion_ts,
    a.first_cancel_ts,
    coalesce(a.first_cancel_ts >= a.first_trial_ts and a.first_cancel_ts < timestamp_add(a.first_trial_ts, interval 1 day), false) as cancelled_d1,
    coalesce(a.first_cancel_ts >= a.first_trial_ts and a.first_cancel_ts < timestamp_add(a.first_trial_ts, interval 7 day), false) as cancelled_d7,
    coalesce(
        a.first_trial_ts is not null
        and j.first_payment_ts is not null
        and a.first_conversion_ts >= a.first_trial_ts
        and a.first_conversion_ts < timestamp_add(
            a.first_trial_ts, interval {{ var('ab_test_conversion_window_days', 7) }} day
        ),
        false
    ) as converted_7d,
    coalesce(
        a.first_trial_ts <= timestamp_sub(
            current_timestamp(), interval {{ var('ab_test_maturity_days', 8) }} day
        ),
        false
    ) as is_mature
from exposures as x
left join after_exposure as a
    on a.product = x.product
    and a.experiment_flag_key = x.experiment_flag_key
    and a.ab_user_id = x.ab_user_id
left join journey as j
    on j.product = x.product
    and j.user_key = x.ab_user_id
