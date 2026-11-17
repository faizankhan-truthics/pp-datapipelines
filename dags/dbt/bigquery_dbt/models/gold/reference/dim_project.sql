{{ config(
    tags=["reference", "gold", "daily"],
) }}

-- Canonical Project list for the dashboard's Project filter: the is_dashboard_project rows of
-- seed_ga4_payment_account_map -- kept as its own model (rather than referencing the seed
-- directly) so a future join (e.g. team ownership) can be added here without touching the seed
-- or any downstream consumer.

select
    ga4_property_name,
    ga4_property_id,
    project_name,
    product_name as domain,
    product_type,
    funnel_signal,
    has_ga4_export,
    ga4_export_exclusion_reason as exclusion_reason
from {{ ref('seed_ga4_payment_account_map') }}
where is_dashboard_project
