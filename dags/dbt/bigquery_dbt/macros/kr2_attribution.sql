-- Helpers for the KR2 attribution taxonomy:
-- seeds/seed_kr2_channel_rules.csv (the rules), seed_kr2_domain_lists.csv (source / referrer
-- lists). The seeds are read at compile time, so a model that calls these macros must declare the
-- dependency itself:
--     -- depends_on: {{ ref('seed_kr2_channel_rules') }}  (and seed_kr2_domain_lists)


{#- The loaded table of a KR2 seed. Looked up in the graph, not through ref(): inside a unit test
    ref() names the test's fixture CTE, while the rules are compiled from the real seed. -#}
{% macro kr2_seed_relation(seed_name) %}
    {%- set node = (graph.get('nodes') or {}).get('seed.' ~ project_name ~ '.' ~ seed_name) -%}
    {%- if node -%}
        {{ return(api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias)) }}
    {%- endif -%}
    {#- no graph (unit tests): where dbt_project.yml puts the project's seeds -#}
    {{ return(api.Relation.create(database=target.database,
                                  schema=env_var('AIRFLOW_VAR_BQ_BRONZE_DATASET', 'placeholder_dataset'),
                                  identifier=seed_name)) }}
{% endmacro %}


{#- The values of one list in seed_kr2_domain_lists ([] while parsing). -#}
{% macro kr2_list_values(list_name, match_type) %}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}
    {% set query %}
        select value from {{ kr2_seed_relation('seed_kr2_domain_lists') }}
        where list_name = '{{ list_name }}' and match_type = '{{ match_type }}'
        order by value
    {% endset %}
    {{ return(run_query(query).columns[0].values() | list) }}
{% endmacro %}


{#- SQL in-list body for a list of strings: 'a', 'b'. An empty list gives a value no label has. -#}
{% macro kr2_in_list(values) %}
    {%- if values | length == 0 -%}
        '__none__'
    {%- else -%}
        {%- for v in values -%}'{{ v | replace("'", "\\'") }}'{{ ', ' if not loop.last }}{%- endfor -%}
    {%- endif -%}
{% endmacro %}


{#- A source / medium label as the rules read it: lowercased and trimmed; blank, (none),
    (not set), unknown, unattributed and Amplitude's EMPTY are no value; GA4's (direct) is direct;
    a malformed label (a URL fragment such as google&utm_medium) is no value too, and
    kr2_is_malformed_label() says so. -#}
{% macro kr2_normalize_label(col) %}
    case
        when nullif(lower(trim({{ col }})), '') is null then null
        when lower(trim({{ col }})) in ('(none)', '(not set)', 'not set', 'none', 'unknown',
                                       'unattributed', 'empty', 'null', 'undefined', '-') then null
        when lower(trim({{ col }})) = '(direct)' then 'direct'
        when {{ kr2_is_malformed_label(col) }} then null
        else lower(trim({{ col }}))
    end
{% endmacro %}


{% macro kr2_is_malformed_label(col) %}
    coalesce(regexp_contains(lower(trim({{ col }})), r'[&=?#{}]|utm_|%[0-9a-f]{2}'), false)
{% endmacro %}


{#- The referring host a touch names in its source: GA4 records a referral as source = the
    referring domain, medium = referral. Null for any other touch. -#}
{% macro kr2_referral_source_domain(source_col, medium_col) %}
    if(lower(trim({{ medium_col }})) = 'referral'
       and regexp_contains(lower(trim({{ source_col }})), r'^[a-z0-9-]+(\.[a-z0-9-]+)+$'),
       lower(trim({{ source_col }})), null)
{% endmacro %}


{#- Referrer class of a host, first match wins: own (the product's site_domains, given as a SQL
    array expression), payment, oauth, intermediary, webmail, search, instagram, ig_com_br, social,
    else external. Null when there is no referrer. `label` entries match a dot-delimited part of the
    host (google matches www.google.co.uk), `host` entries the host or its subdomains. -#}
{% macro kr2_referrer_class(host_col, own_domains_array) %}
    {%- set ns = namespace(cases=[]) -%}
    {%- for cls in ['payment', 'oauth', 'intermediary', 'webmail', 'search', 'instagram', 'ig_com_br', 'social'] -%}
        {%- set labels = kr2_list_values(cls, 'label') -%}
        {%- set hosts = kr2_list_values(cls, 'host') -%}
        {%- set preds = [] -%}
        {%- if labels | length > 0 -%}
            {%- do preds.append("regexp_contains(h, r'(^|\\.)(" ~ (labels | join('|')) ~ ")\\.')") -%}
        {%- endif -%}
        {%- for v in hosts -%}
            {%- do preds.append("(h = '" ~ v ~ "' or ends_with(h, '." ~ v ~ "'))") -%}
        {%- endfor -%}
        {%- if preds -%}
            {%- do ns.cases.append("when " ~ (preds | join(' or ')) ~ " then '" ~ cls ~ "'") -%}
        {%- endif -%}
    {%- endfor -%}
    (select
        case
            when h is null then null
            when exists (select 1 from unnest({{ own_domains_array }}) as d
                         where trim(d) != '' and (h = lower(trim(d)) or ends_with(h, concat('.', lower(trim(d))))))
                then 'own'
            {{ ns.cases | join('\n            ') }}
            else 'external'
        end
     from (select nullif(lower(trim({{ host_col }})), '') as h))
{% endmacro %}


{#- One boolean per condition_key of seed_kr2_channel_rules, over the columns of
    int_user_touch_classified's `prepared` CTE. -#}
{% macro kr2_condition(key) %}
    {%- set conditions = {
        'always':                 'true',
        'selection_only':         'false',
        'internal_return':        "(n_click_platforms = 0 and campaign_norm is null and ((referrer_class in ('payment', 'oauth') and coalesce(med, 'referral') = 'referral') or (referrer_class = 'own' and src is null and med is null)))",
        'qr_product_flow':        "(referrer_class is null or referrer_class = 'own')",
        'google_click_no_source': "(has_google_click and n_click_platforms = 1)",
        'ms_click_no_source':     "(has_ms_click and n_click_platforms = 1 and not is_malformed_label)",
        'platform_conflict':      "(n_click_platforms > 1 or (source_platform is not null and click_platform is not null and source_platform != click_platform))",
        'odd_label_with_click':   "(is_malformed_label and n_click_platforms > 0 and not has_google_click and not is_source_recovered)",
        'google_click':           'has_google_click',
        'ms_click':               'has_ms_click',
        'no_paid_click':          '(not has_google_click and not has_ms_click)',
        'search_referrer':        "(referrer_class = 'search' and not has_google_click and not has_ms_click)",
        'social_referrer':        "(referrer_class in ('social', 'instagram') and not has_google_click and not has_ms_click)",
        'instagram_referrer':     "(referrer_class = 'instagram')",
        'ig_com_br_referrer':     "(referrer_class = 'ig_com_br')",
        'email_campaign':         '(campaign_norm is not null)',
        'affiliate_medium':       "(med in ('affiliate', 'affiliates', 'aff', 'partner', 'partners', 'cpa'))",
        'direct_visit':           "(not is_malformed_label and n_click_platforms = 0 and referrer_class is null and med is null and campaign_norm is null and (src is null or src = 'direct'))",
        'external_referrer':      "(referrer_class = 'external')",
    } -%}
    {%- if key not in conditions -%}
        {{ exceptions.raise_compiler_error("kr2_condition: unknown condition_key '" ~ key ~ "' in seed_kr2_channel_rules") }}
    {%- endif -%}
    {{- conditions[key] -}}
{% endmacro %}


{#- Match of one value column against a rule's match spec:
      *          anything
      ~missing   no value
      ~invalid   no value, or a medium the taxonomy does not know
      @<list>    in a `source` list of seed_kr2_domain_lists (@search_engine)
      !a;b       a value, not a or b
      ^a;b       no value, or not a or b
      a;b        a or b; a `~missing` item in the list also accepts no value -#}
{% macro kr2_match(col, spec) %}
    {%- set spec = (spec or '*') | trim -%}
    {%- if spec == '*' -%}
        true
    {%- elif spec == '~missing' -%}
        {{ col }} is null
    {%- elif spec == '~invalid' -%}
        ({{ col }} is null or {{ col }} not in ('cpc', 'ppc', 'paid', 'display', 'organic', 'social',
                                               'email', 'newsletter', 'referral', 'affiliate',
                                               'affiliates', 'aff', 'partner', 'partners', 'cpa'))
    {%- elif spec.startswith('@') -%}
        {{ col }} in ({{ kr2_in_list(kr2_list_values(spec[1:], 'source')) }})
    {%- elif spec.startswith('!') -%}
        ({{ col }} is not null and {{ col }} not in ({{ kr2_in_list(spec[1:].split(';') | map('trim') | list) }}))
    {%- elif spec.startswith('^') -%}
        ({{ col }} is null or {{ col }} not in ({{ kr2_in_list(spec[1:].split(';') | map('trim') | list) }}))
    {%- else -%}
        {%- set items = spec.split(';') | map('trim') | list -%}
        {%- set values = items | reject('equalto', '~missing') | list -%}
        ({{ col }} in ({{ kr2_in_list(values) }}){{ ' or ' ~ col ~ ' is null' if '~missing' in items }})
    {%- endif -%}
{% endmacro %}


{#- The rules of seed_kr2_channel_rules in check_order, as dicts ([] while parsing). -#}
{% macro kr2_rules() %}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}
    {% set result = run_query("select * from " ~ kr2_seed_relation('seed_kr2_channel_rules') ~ " order by check_order") %}
    {% set rules = [] %}
    {% for r in result.rows %}
        {% set row = {} %}
        {% for col in result.column_names %}
            {% do row.update({col: r[col]}) %}
        {% endfor %}
        {% do rules.append(row) %}
    {% endfor %}
    {{ return(rules) }}
{% endmacro %}


{#- Predicate of one rule over `product`, `src`, `med`, `net` and the condition columns. -#}
{% macro kr2_rule_predicate(rule) %}
    ({{ kr2_match('product', rule.product_match) }}
     and {{ kr2_match('src', rule.source_match) }}
     and {{ kr2_match('med', rule.medium_match) }}
     and {{ kr2_match('net', rule.network_match) }}
     and {{ kr2_condition(rule.condition_key) }})
{% endmacro %}


{#- rule_id of the first rule a touch matches, in check_order. `recover` rules are applied
    before (kr2_recovered_source) and selection_only rules by the journey, so neither is here.
    N17 matches anything, so every touch gets a rule. -#}
{% macro kr2_rule_case() %}
    case
    {%- for r in kr2_rules() if r.decision != 'recover' and r.condition_key != 'selection_only' %}
        when {{ kr2_rule_predicate(r) }} then '{{ r.rule_id }}'
    {%- endfor %}
    end
{% endmacro %}


{#- The source the first matching `recover` rule sets (R01 google, R02 bing), else null. -#}
{% macro kr2_recovered_source() %}
    {%- set sources = {'google_click_no_source': 'google', 'ms_click_no_source': 'bing'} -%}
    {%- set rules = kr2_rules() | selectattr('decision', 'equalto', 'recover') | list -%}
    {%- if not rules -%}
        cast(null as string)
    {%- else -%}
        case
        {%- for r in rules %}
            when {{ kr2_rule_predicate(r) }} then '{{ sources[r.condition_key] }}'
        {%- endfor %}
        end
    {%- endif -%}
{% endmacro %}
