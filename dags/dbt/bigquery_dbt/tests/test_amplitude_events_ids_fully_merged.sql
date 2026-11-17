-- depends_on: {{ ref('seed_product_registry') }}
-- int_amplitude_events must store the last id of each merge chain: no stored amplitude_id may
-- still be merged into another id in MERGE_USERS_<project> (catches one-hop chain resolution).
-- Warn only: a merge exported after the model ran is picked up by the next run.
{{ config(severity='warn') }}

{% set products = registry_products('amplitude_source_table') %}

with merged as (

{% for p in products %}
    select distinct '{{ p.product }}' as product, cast(amplitude_id as string) as amplitude_id
    from {{ source('amplitude', p.amplitude_source_table | replace('events_', 'merge_users_')) }}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% endfor %}

)

select e.product, e.amplitude_id, count(*) as events
from {{ ref('int_amplitude_events') }} as e
inner join merged as m
    on m.product = e.product and m.amplitude_id = e.amplitude_id
group by 1, 2
