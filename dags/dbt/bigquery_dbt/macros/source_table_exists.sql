{#- True when the table behind a source exists in the warehouse. Tests on tables that an Airflow DAG creates
    (e.g. bronze.growthbook_*) use it to return no rows, instead of a "Not found" error, in an environment
    where the DAG has not run yet. `severity: warn` does not cover that error. While dbt is only parsing
    (`execute` is false) it answers true, so the real query is still compiled and the source is still linked. -#}
{% macro source_table_exists(source_name, table_name) %}
    {%- if not execute -%}
        {{ return(true) }}
    {%- endif -%}
    {%- set rel = source(source_name, table_name) -%}
    {{ return(adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) is not none) }}
{% endmacro %}
