-- Helpers over seeds/seed_product_registry.csv, the per-product config of the user journey mart.
-- A model that calls registry_products() must declare the dependency itself, because the ref
-- inside run_query is not seen at parse time:
--     -- depends_on: {{ ref('seed_product_registry') }}


{#
  In-scope registry rows as a list of dicts (column name -> value), ordered by product.
  `required_column`: keep only rows where that column is filled (e.g. 'amplitude_source_table'
  for models that read Amplitude, 'ga4_property_name' for GA4 ones).
  Returns [] at parse time.
#}
{% macro registry_products(required_column=none) %}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}

    {% set query %}
        select *
        from {{ ref('seed_product_registry') }}
        where is_in_scope
        {% if required_column %}
          and nullif(trim(cast({{ required_column }} as string)), '') is not null
        {% endif %}
        order by product
    {% endset %}

    {% set result = run_query(query) %}
    {% set rows = [] %}
    {% for r in result.rows %}
        {% set row = {} %}
        {% for col in result.column_names %}
            {% do row.update({col: r[col]}) %}
        {% endfor %}
        {% do rows.append(row) %}
    {% endfor %}
    {{ return(rows) }}
{% endmacro %}


{#
  Turns a registry rule string into a SQL predicate for one source.
  Grammar: `source:event_name[prop=value]`, rules separated by `;`.
    registry_event_filter('amplitude:signup_complete;ga4:signup', 'amplitude')
      -> (event_type = 'signup_complete')
    registry_event_filter('amplitude:page_view[screen_id=final_report]', 'amplitude')
      -> (event_type = 'page_view' and screen_id = 'final_report')
  Rules for other sources are ignored; no matching rule -> `false`.
  `prop=value` is compared against a column of the same name, so the model must expose the
  property as a column (int_amplitude_events extracts screen_id, signup_type, ...).
#}
{% macro registry_event_filter(rules, source, event_column='event_type') %}
    {%- set predicates = [] -%}
    {%- for raw_rule in (rules or '').split(';') -%}
        {%- set rule = raw_rule | trim -%}
        {%- if rule and ':' in rule -%}
            {%- set rule_source = rule.split(':', 1)[0] | trim -%}
            {%- set body = rule.split(':', 1)[1] | trim -%}
            {%- if rule_source == source -%}
                {%- if '[' in body -%}
                    {%- set event_name = body.split('[', 1)[0] | trim -%}
                    {%- set condition = body.split('[', 1)[1].rstrip(']') -%}
                    {%- set prop = condition.split('=', 1)[0] | trim -%}
                    {%- set value = condition.split('=', 1)[1] | trim -%}
                    {%- do predicates.append(
                        "(" ~ event_column ~ " = '" ~ event_name ~ "' and " ~ prop ~ " = '" ~ value ~ "')"
                    ) -%}
                {%- else -%}
                    {%- do predicates.append("(" ~ event_column ~ " = '" ~ body ~ "')") -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {{- '(' ~ (predicates | join(' or ')) ~ ')' if predicates else 'false' -}}
{% endmacro %}


{#
  `;`-separated registry list column -> SQL in-list body: 'a', 'b', 'c'.
  Used for ga4_checkout_events and site_domains.
#}
{% macro registry_in_list(value) %}
    {%- set items = [] -%}
    {%- for raw in (value or '').split(';') -%}
        {%- if raw | trim -%}
            {%- do items.append("'" ~ (raw | trim) ~ "'") -%}
        {%- endif -%}
    {%- endfor -%}
    {{- items | join(', ') if items else "''" -}}
{% endmacro %}
