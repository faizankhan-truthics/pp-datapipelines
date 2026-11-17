-- only meaningful while the KR2 taxonomy is on (kr2_taxonomy_enabled)
{{ config(enabled=var('kr2_taxonomy_enabled', true)) }}

-- the KR2 columns of a journey must agree: unattributed <=> rule J01, an unattributed journey has
-- method none and is always on the review list, as is a fallback journey. (An attributed journey
-- can have method none too: a direct visit that carries no evidence, D1.)
select product, user_key, channel_group, attribution_rule_id, source_resolution_method,
       is_fallback_attribution, needs_attribution_review
from {{ ref('dim_user_journey') }}
where (channel_group = 'unattributed') != (attribution_rule_id = 'J01')
   or (channel_group = 'unattributed' and source_resolution_method != 'none')
   or (channel_group = 'unattributed' and not needs_attribution_review)
   or (source_resolution_method = 'none' and channel_group not in ('unattributed', 'direct'))
   or (is_fallback_attribution and (channel_group = 'unattributed' or not needs_attribution_review))
