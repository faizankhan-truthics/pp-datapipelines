{{ config(
    materialized='table',
    tags=["amplitude", "ab_test", "gold", "daily"],
    cluster_by=['experiment_flag_key', 'ab_user_id']
) }}

-- OpenGen-only metrics for the A/B test dashboard, one row per OpenGen user in fct_ab_test_users
-- (same key). Join it to fct_ab_test_users on product, experiment_flag_key and ab_user_id.
-- credit_topup_in_trial: a credits_purchased event during the trial, from the first trial to the
-- earliest of first conversion, first cancel and first trial + ab_test_trial_days (default 5).
-- False without a trial. The conversion used is the payment-confirmed one from fct_ab_test_users.
-- n_generation_*: generation events at or after the first exposure.
-- active_d1 / active_d7: a generation_started on the calendar day 1 / 7 after the first trial.
-- Events are read from ab_test_min_date (default 2026-10-01).
-- Grain: product x experiment_flag_key x ab_user_id.

with spine as (

    select
        product,
        experiment_flag_key,
        ab_user_id,
        first_exposure_ts,
        first_trial_ts,
        first_conversion_ts,
        first_cancel_ts
    from {{ ref('fct_ab_test_users') }}
    where product = 'opengen_ai'

),

resolution as (

    select product, id_type, id_value, user_key
    from {{ ref('int_identity_resolution') }}
    where product = 'opengen_ai'
      and id_type in ('app_user_id', 'amplitude_id')

),

-- the same ab_user_id as fct_ab_test_users: the user_key of the event's app user id or Amplitude id
ev as (

    select
        coalesce(ra.user_key, rb.user_key, r.app_user_id, r.amplitude_id) as ab_user_id,
        r.event_type,
        r.event_ts
    from {{ ref('int_amplitude_events') }} as r
    left join resolution as ra
        on ra.id_type = 'app_user_id'
        and ra.id_value = r.app_user_id
    left join resolution as rb
        on rb.id_type = 'amplitude_id'
        and rb.id_value = r.amplitude_id
    where r.product = 'opengen_ai'
      and r.event_date >= date('{{ var("ab_test_min_date", "2026-10-01") }}')
      and r.event_type in (
          'credits_purchased', 'generation_started', 'generation_completed', 'generation_failed'
      )

)

select
    s.product,
    s.experiment_flag_key,
    s.ab_user_id,
    coalesce(logical_or(
        e.event_type = 'credits_purchased'
        and s.first_trial_ts is not null
        and e.event_ts >= s.first_trial_ts
        and e.event_ts < least(
            timestamp_add(s.first_trial_ts, interval {{ var('ab_test_trial_days', 5) }} day),
            coalesce(s.first_conversion_ts, timestamp('9999-12-31')),
            coalesce(s.first_cancel_ts, timestamp('9999-12-31'))
        )
    ), false) as credit_topup_in_trial,
    countif(e.event_type = 'generation_started') as n_generation_started,
    countif(e.event_type = 'generation_completed') as n_generation_completed,
    countif(e.event_type = 'generation_failed') as n_generation_failed,
    coalesce(logical_or(
        e.event_type = 'generation_started'
        and date(e.event_ts) = date_add(date(s.first_trial_ts), interval 1 day)
    ), false) as active_d1,
    coalesce(logical_or(
        e.event_type = 'generation_started'
        and date(e.event_ts) = date_add(date(s.first_trial_ts), interval 7 day)
    ), false) as active_d7
from spine as s
left join ev as e
    on e.ab_user_id = s.ab_user_id
    and e.event_ts >= s.first_exposure_ts
group by s.product, s.experiment_flag_key, s.ab_user_id
