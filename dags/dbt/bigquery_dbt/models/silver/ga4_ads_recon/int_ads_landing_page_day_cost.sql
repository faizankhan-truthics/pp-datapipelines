{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='domain_funnel_customer_day_key',
    on_schema_change='append_new_columns',
    partition_by={
        "field": "date",
        "data_type": "date"
    }
    )
}}

-- Ad spend split by destination domain (project), not just campaign_id -- campaigns/accounts
-- have no project dimension of their own (confirmed: int_ads_campaign_day_cost is campaign_id
-- x date only), but the landing page URL each ad group actually sends clicks to does, and that
-- maps 1:1 to a project via seed_ga4_payment_account_map.product_name (dim_project.domain).
--
-- Deliberately NOT joined to campaign_id: campaign_name in ads_LandingPageStats is not unique,
-- not even within one customer_id (the same name is reused across different campaign_ids --
-- confirmed empirically), so matching on name would fan out and inflate cost. Aggregating
-- straight to domain x date avoids that risk entirely; campaign-level drill-down isn't this
-- model's job.
--
-- customer_id is kept IN the grain, not collapsed -- confirmed empirically that a single
-- domain can span several Ads sub-accounts (publicregistry.us alone runs 6: Court, Financial,
-- Crime, Property, Vital, People & Property). They're all USD today, but collapsing to one
-- any_value(customer_id) per domain would silently misattribute currency on the day a
-- different sub-account's cost actually landed, the moment one of them stops being USD.
-- Currency conversion must happen per customer_id, before summing across customers -- that
-- happens in project_roas_daily.sql, which is also why customer_id is kept here rather than
-- resolved to currency_code in this model.
--
-- Same snapshot-revision risk as int_ads_campaign_day_cost (Ads Data Transfer re-states
-- historical days under a new _DATA_DATE) -- dedup to the latest snapshot per natural key
-- before summing, same pattern.
--
-- funnel is the landing-page URL's product/topic segment (walking, phone-lookup, adhd, track,
-- ...), with the language prefix (/en/, /pt-br/, /yue/, /fil/ -- 2 or 3 letters) and the /quiz/ or /start/ wrapper stripped --
-- the Funnel definition Manya's URL sheet specifies, not the post-click Search->Paywall /
-- Quiz->Paywall / Trial->Subscription stage (that one can't be derived from ad data at all). It
-- is part of the grain so spend can be split by it; consumers that only want domain x date
-- (project_roas_daily) just sum across it. The slug is kept raw (e.g. calisthenics-m and
-- calisthenics-f stay distinct) rather than collapsed to a hand-maintained allow-list -- the
-- gold mart is where unrecognised values get surfaced. Bare language roots ('/en') and
-- slug-less URLs land in '(root)'.
--
-- Grain: domain x funnel x customer_id x date.

{% set rescan_days = 4 %}

with latest_snapshot as (

    select
        customer_id, campaign_name, ad_group_name, landing_page_view_unexpanded_final_url,
        segments_date,
        max(_data_date) as latest_data_date
    from {{ source('bronze_google_ads', 'ads_LandingPageStats_2747379648') }}
    {% if is_incremental() %}
    where segments_date >= date_sub(current_date(), interval {{ rescan_days }} day)
    {% endif %}
    group by 1, 2, 3, 4, 5

),

landing_page_cost as (

    select
        l.customer_id,
        l.segments_date as date,
        regexp_extract(
            l.landing_page_view_unexpanded_final_url, r'https?://(?:www\.)?([^/?#]+)'
        ) as domain,
        lower(regexp_extract(
            l.landing_page_view_unexpanded_final_url, r'https?://(?:www\.)?[^/?#]+(/[^?#]*)'
        )) as path,
        l.metrics_cost_micros
    from {{ source('bronze_google_ads', 'ads_LandingPageStats_2747379648') }} l
    join latest_snapshot s
        on  l.customer_id = s.customer_id
        and l.campaign_name = s.campaign_name
        and l.ad_group_name = s.ad_group_name
        and l.landing_page_view_unexpanded_final_url = s.landing_page_view_unexpanded_final_url
        and l.segments_date = s.segments_date
        and l._data_date = s.latest_data_date
    {% if is_incremental() %}
    where l.segments_date >= date_sub(current_date(), interval {{ rescan_days }} day)
    {% endif %}

),

parsed_path as (

    select
        domain,
        customer_id,
        date,
        metrics_cost_micros,
        case
            -- bare language root ('/en', '/pt-br/') would otherwise surface as the funnel 'en'
            when path is null or regexp_contains(path, r'^/(?:[a-z]{2,3}(?:-[a-z]{2})?)?/?$') then '(root)'
            else coalesce(
                regexp_extract(path, r'^/(?:[a-z]{2,3}(?:-[a-z]{2})?/)?(?:quiz/|start/)?([^/?#]+)'),
                '(root)'
            )
        end as path_funnel
    from landing_page_cost
    where domain is not null

),

with_funnel as (

    select
        domain,
        customer_id,
        date,
        metrics_cost_micros,
        case
            -- Reversly's /en/image is the same funnel as /image-lookup (confirmed by Borys via
            -- Manya). Scoped to this domain on purpose: other domains (chat-box.ai, simpl.ai) use
            -- /image for unrelated products. The IQ Center *-test slugs were confirmed as
            -- separate funnels and are deliberately NOT merged.
            when domain = 'reversly.com' and path_funnel = 'image' then 'image-lookup'
            else path_funnel
        end as funnel
    from parsed_path

)

select
    domain,
    funnel,
    customer_id,
    date,
    concat(domain, '-', funnel, '-', cast(customer_id as string), '-', cast(date as string)) as domain_funnel_customer_day_key,
    sum(metrics_cost_micros) / 1e6 as ads_cost
from with_funnel
group by domain, funnel, customer_id, date
