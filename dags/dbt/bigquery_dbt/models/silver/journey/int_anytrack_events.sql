-- depends_on: {{ ref('seed_product_registry') }}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='anytrack_event_key',
    on_schema_change='append_new_columns',
    tags=["journey", "anytrack", "silver", "daily"],
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=['product', 'email_user_key']
) }}

-- The AnyTrack events of every in-scope product in seed_product_registry that carry an email:
-- signups, trials and purchases. They give the journey mart
--   identity     the email (md5 only, the user_key of int_identity_user_map), which links an AnyTrack
--                visitor to its payer / app user, and is the user record of products without
--                Amplitude (E17)
--   stages       signup / trial events named in the registry rules (`anytrack:<event_name>`)
--   app user     the app account id a product sends as the signup's transaction_id
--                (`signup:<id>`, opengen_ai), which ties the email to its app user (E18)
--   first touch  main_attribution, AnyTrack's first touch of the visitor (it equals the earliest entry
--                of `attributions`): UTMs, referrer, landing URL, click ids and the ValueTrack network
-- Events without an email (page steps, uploads) are left out: they belong to the same visitor as its
-- signup, whose main_attribution is the same.
-- A product may have several AnyTrack tables (files_editor;file_editor). A table that does not exist
-- in this environment is skipped. Increments are picked per table by _elt_update_timestamp; the
-- same event delivered by webhook and by CSV backfill is kept once, preferring the webhook row.
-- Grain: one row per AnyTrack event (anytrack_event_key).

{%- set selects = [] %}
{%- for p in registry_products('anytrack_product') %}
    {%- for t in (p.anytrack_product or '').split(';') if t | trim %}
        {%- set table = 'anytrack_' ~ (t | trim) ~ '_events' %}
        {%- if source_table_exists('bronze_anytrack', table) %}
            {%- do selects.append((p.product, t | trim, table)) %}
        {%- endif %}
    {%- endfor %}
{%- endfor %}

with raw as (

{% for product, anytrack_table, table in selects %}
    select
        '{{ product }}'                                             as product,
        '{{ anytrack_table }}'                                      as anytrack_table,
        event_name,
        event_time                                                  as event_ts,
        nullif(trim(click_id), '')                                  as click_id,
        nullif(trim(transaction_id), '')                            as transaction_id,
        to_hex(md5(lower(trim(email))))                             as email_user_key,
        event_value,
        currency,
        main_attribution                                            as ma,
        _source,
        _elt_update_timestamp
    from {{ source('bronze_anytrack', table) }}
    where nullif(lower(trim(email)), '') is not null
      and event_time is not null
    {% if is_incremental() %}
      and _elt_update_timestamp > (
          select coalesce(max(_elt_update_timestamp), timestamp('1970-01-01'))
          from {{ this }}
          where anytrack_table = '{{ anytrack_table }}'
      )
    {% endif %}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% else %}
    -- no AnyTrack table in this environment
    select
        cast(null as string) as product, cast(null as string) as anytrack_table,
        cast(null as string) as event_name, cast(null as timestamp) as event_ts,
        cast(null as string) as click_id, cast(null as string) as transaction_id,
        cast(null as string) as email_user_key, cast(null as float64) as event_value,
        cast(null as string) as currency, cast(null as json) as ma,
        cast(null as string) as _source, cast(null as timestamp) as _elt_update_timestamp
    from unnest([1])
    where false
{% endfor %}

),

keyed as (

    select
        to_hex(md5(concat(
            product, '|', coalesce(click_id, ''), '|', event_name, '|',
            cast(unix_micros(event_ts) as string), '|', coalesce(transaction_id, '')
        )))                                                         as anytrack_event_key,
        *
    from raw
    qualify row_number() over (
        partition by product, click_id, event_name, event_ts, transaction_id
        order by if(_source = 'csv_backfill', 1, 0), _elt_update_timestamp desc
    ) = 1

)

select
    anytrack_event_key,
    product,
    anytrack_table,
    date(event_ts)                                                  as event_date,
    event_ts,
    event_name,
    click_id,
    transaction_id,
    email_user_key,
    regexp_extract(transaction_id, r'^signup:(.+)$')                as signup_app_user_id,
    event_value,
    currency,

    -- first touch (main_attribution)
    timestamp(json_value(ma, '$.time'))                             as touch_ts,
    nullif(trim(json_value(ma, '$.source')), '')                    as touch_source,
    nullif(trim(json_value(ma, '$.medium')), '')                    as touch_medium,
    nullif(trim(json_value(ma, '$.campaign')), '')                  as touch_campaign,
    nullif(trim(json_value(ma, '$.term')), '')                      as touch_term,
    nullif(trim(json_value(ma, '$.content')), '')                   as touch_content,
    net.host(nullif(trim(json_value(ma, '$.referrer')), ''))        as touch_referrer_domain,
    regexp_replace(json_value(ma, '$.location'), r'[?#].*$', '')    as touch_landing_page,
    nullif(trim(json_value(ma, '$.identifier')), '')                as touch_click_id_type,
    nullif(trim(json_value(ma, '$.gclid')), '')                     as gclid,
    coalesce(nullif(trim(json_value(ma, '$.gbraid')), ''),
             nullif(trim(json_value(ma, '$.params.gbraid')), ''))   as gbraid,
    coalesce(nullif(trim(json_value(ma, '$.wbraid')), ''),
             nullif(trim(json_value(ma, '$.params.wbraid')), ''))   as wbraid,
    coalesce(nullif(trim(json_value(ma, '$.msclkid')), ''),
             nullif(trim(json_value(ma, '$.params.msclkid')), ''))  as msclkid,
    coalesce(nullif(trim(json_value(ma, '$.fbclid')), ''),
             nullif(trim(json_value(ma, '$.params.fbclid')), ''))   as fbclid,
    coalesce(nullif(trim(json_value(ma, '$.ttclid')), ''),
             nullif(trim(json_value(ma, '$.params.ttclid')), ''))   as ttclid,
    -- Google / Microsoft ValueTrack network (g, s, x, d, o, a, ...); an unexpanded placeholder
    -- ({network}) is no value
    if(starts_with(json_value(ma, '$.params.network'), '{'), null,
       nullif(trim(json_value(ma, '$.params.network')), ''))        as ad_network,

    _source,
    _elt_update_timestamp
from keyed
{% if is_incremental() %}
-- a CSV copy of an event already stored from the webhook would overwrite it in the merge: the
-- preference above only sees the current batch
where not (
    _source = 'csv_backfill'
    and anytrack_event_key in (
        select anytrack_event_key from {{ this }} where coalesce(_source, '') != 'csv_backfill'
    )
)
{% endif %}
