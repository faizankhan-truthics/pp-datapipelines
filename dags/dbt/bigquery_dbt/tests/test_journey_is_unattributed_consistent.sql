-- only meaningful once the KR2 [1/5] taxonomy is switched on (kr2_taxonomy_enabled)
{{ config(enabled=var('kr2_taxonomy_enabled', true)) }}

select product, user_key, channel_group, is_unattributed
from {{ ref('dim_user_journey') }}
where is_unattributed != (channel_group = 'unattributed')
   or (is_unattributed is null) != (channel_group is null)
