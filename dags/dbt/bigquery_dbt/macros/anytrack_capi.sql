{#- Products the Conversion API backfill (DAG anytrack_capi_events) can write to: the keys of CAPI_PROPERTIES in
    include/config/anytrack_settings.py. Keep the two in sync, and declare each product's
    anytrack_<product>_events table in models/bronze/anytrack.yml. -#}
{% macro anytrack_capi_products() %}
    {{ return([
        "iqcenter",
        "infochecker",
        "files_editor_preprod",
        "files_editor",
        "file_editor",
        "tracelo",
        "reversly",
        "opengen_ai",
        "giftmusic",
        "oqg",
        "location_tool",
        "qci",
        "qga",
        "chatbox_ai",
        "simpl_ai",
        "publicregistry",
        "fitday",
        "docuhero",
    ]) }}
{% endmacro %}

{#- Runs the block once per backfillable product whose bronze table exists, and `union all`s the results.
    The block receives the source relation and the product name:

        {% call(events, product) anytrack_capi_each_product() %}
            select '{{ product }}' as product, ... from {{ events }}
        {% endcall %}

    A table that does not exist yet (the DAG has not run in this environment), or that still has the older
    schema without `_source` / `_raw_payload` (it cannot hold backfilled rows), is skipped, so the test
    returns no rows for it instead of a database error. -#}
{% macro anytrack_capi_each_product() %}
    {%- set parts = [] -%}
    {%- for product in anytrack_capi_products() -%}
        {%- set table = 'anytrack_' ~ product ~ '_events' -%}
        {%- if source_table_exists('bronze_anytrack', table) -%}
            {%- set relation = source('bronze_anytrack', table) -%}
            {%- set has_backfill_columns = true -%}
            {%- if execute -%}
                {%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
                {%- set has_backfill_columns = '_source' in columns and '_raw_payload' in columns -%}
            {%- endif -%}
            {%- if has_backfill_columns -%}
                {%- do parts.append(caller(relation, product)) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if parts | length == 0 -%}
        select cast(null as string) as product where false
    {%- else -%}
        {{ parts | join('\n    union all\n') }}
    {%- endif -%}
{% endmacro %}
