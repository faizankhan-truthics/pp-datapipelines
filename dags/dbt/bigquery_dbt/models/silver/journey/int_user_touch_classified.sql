-- depends_on: {{ ref('seed_kr2_channel_rules') }}
-- depends_on: {{ ref('seed_kr2_domain_lists') }}
{{ config(
    materialized='table',
    tags=["journey", "kr2", "silver", "daily"],
    cluster_by=['product', 'user_key']
) }}

-- Every touch candidate with the KR2 rule it falls under (seeds/seed_kr2_channel_rules.csv): the
-- first rule in check_order whose product / source / medium / network match and condition hold.
-- The rules are compiled into one CASE from the seed (macros/kr2_attribution.sql), so changing a
-- rule is a seed change.
-- `recover` rules run first and set the source from the click id when the touch has none -- R01
-- google (Google click id, also for a malformed source), R02 bing (Microsoft click id); the touch is
-- then classified like any other (is_source_recovered).
-- decision: ignore (not an acquisition touch), skip_flag (ambiguous: skipped and logged, with the
-- fallback_channel_group a journey with no assigned touch takes), assign (channel_group).
-- dim_user_journey chooses the journey's touch from these.
-- Grain: product x user_key x touch_system x touch_id (as int_user_touch_candidates).

with candidates as (

    select * from {{ ref('int_user_touch_candidates') }}

),

recovered as (

    select
        *,
        {{ kr2_recovered_source() }}                                    as recovered_source
    from (select *, ad_network as net from candidates)

),

prepared as (

    select
        * except (src, recovered_source),
        recovered_source is not null                                    as is_source_recovered,
        coalesce(recovered_source, src)                                 as src
    from recovered

),

ruled as (

    select
        *,
        {{ kr2_rule_case() }}                                           as rule_id
    from prepared

)

select
    r.product,
    r.user_key,
    r.touch_system,
    r.touch_id,
    r.touch_ts,
    r.is_touch_date_only,
    r.source,
    r.medium,
    r.campaign,
    r.term,
    r.referrer_domain,
    r.landing_page,
    r.ad_network,
    r.click_id_type,
    r.click_id,
    r.src                                                               as source_norm,
    r.med                                                               as medium_norm,
    r.referrer_class,
    r.is_malformed_label,
    r.is_source_recovered,
    r.n_click_platforms,
    r.source_resolution_method,
    r.method_rank,
    r.rule_id,
    s.decision,
    s.channel_group,
    if(s.decision = 'skip_flag', s.fallback_channel_group, null)        as fallback_channel_group,
    if(s.decision = 'skip_flag', s.reason, null)                        as review_reason
from ruled r
left join {{ ref('seed_kr2_channel_rules') }} s on s.rule_id = r.rule_id
