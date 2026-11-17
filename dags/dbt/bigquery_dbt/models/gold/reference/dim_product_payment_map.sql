{{ config(
    tags=["reference", "gold", "daily"],
) }}

with amplitude_prod as (

    select ga4_property_name, amplitude_project_id
    from {{ ref('seed_amplitude_project_map') }}
    where environment = 'prod'

),

ga4_by_product as (

    select
        s.product_name,
        s.ga4_property_id,
        s.posthog_id,
        a.amplitude_project_id
    from {{ ref('seed_ga4_payment_account_map') }} s
    left join amplitude_prod a
        on a.ga4_property_name = s.ga4_property_name
    where s.product_name is not null
      and (s.ga4_property_id is not null or s.posthog_id is not null)

),

stripe_accounts as (

    select
        'stripe' as payment_provider,
        a.account as psp_account_name,
        a.id as stripe_account_id,
        cast(null as string) as solidgate_account_name,
        a.product as product_name,
        'stripe_accounts.product' as product_name_source,
        g.ga4_property_id,
        g.posthog_id,
        g.amplitude_project_id

    from {{ source('bronze_stripe', 'stripe_accounts') }} a
    left join ga4_by_product g
        on g.product_name = a.product

),

solidgate_accounts as (

    select _elt_account_name as account_name
    from {{ source('bronze_solidgate', 'solidgate_card_orders') }}
    group by 1

    union distinct

    select _elt_account_name
    from {{ source('bronze_solidgate', 'apm_orders') }}
    group by 1

    union distinct

    select _elt_account_name
    from {{ source('bronze_solidgate', 'subscriptions') }}
    group by 1

),

ga4_solidgate_map as (

    select
        s.account_key,
        s.ga4_property_name,
        s.product_name,
        s.ga4_property_id,
        s.posthog_id,
        a.amplitude_project_id
    from {{ ref('seed_ga4_payment_account_map') }} s
    left join amplitude_prod a
        on a.ga4_property_name = s.ga4_property_name
    where s.payment_source = 'solidgate'
      and s.account_key is not null

),

solidgate_final as (

    select
        'solidgate' as payment_provider,
        s.account_name as psp_account_name,
        cast(null as string) as stripe_account_id,
        s.account_name as solidgate_account_name,
        g.product_name,
        case when g.product_name is not null
            then 'seed_ga4_payment_account_map.product_name'
        end as product_name_source,
        g.ga4_property_id,
        g.posthog_id,
        g.amplitude_project_id

    from solidgate_accounts s
    left join ga4_solidgate_map g
        on g.account_key = s.account_name

)

select
    payment_provider,
    psp_account_name,
    stripe_account_id,
    solidgate_account_name,
    product_name,
    product_name_source,
    ga4_property_id,
    posthog_id,
    amplitude_project_id,
    cast(null as string) as team_name

from stripe_accounts

union all

select
    payment_provider,
    psp_account_name,
    stripe_account_id,
    solidgate_account_name,
    product_name,
    product_name_source,
    ga4_property_id,
    posthog_id,
    amplitude_project_id,
    cast(null as string) as team_name

from solidgate_final
