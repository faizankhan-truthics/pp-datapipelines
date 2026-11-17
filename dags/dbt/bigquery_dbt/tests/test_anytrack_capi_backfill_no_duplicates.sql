-- warn: one severity covers both checks, and the second can hit two real events, so a result is a
-- prompt to look, not proof of a duplicate.
{{ config(severity='warn') }}

-- Runs for every backfillable product. Each row returned is a problem:
--   duplicate_transaction_id       a transaction_id on more than one row (the webhook MERGE and the
--                                  Conversion API backfill both key on it; null ids may repeat)
--   near_duplicate_of_webhook_row  a capi_backfill row with the same client_id and event_name as a
--                                  webhook row within 10 seconds (the tolerance the backfill uses)
{% call(events, product) anytrack_capi_each_product() %}
    select '{{ product }}' as product, 'duplicate_transaction_id' as problem,
           cast(transaction_id as string) as detail, count(*) as n
    from {{ events }}
    where transaction_id is not null
    group by transaction_id
    having count(*) > 1

    union all

    select '{{ product }}' as product, 'near_duplicate_of_webhook_row' as problem,
           concat(b.client_id, ' | ', b.event_name) as detail, count(*) as n
    from {{ events }} b
    join {{ events }} w
      on w.client_id = b.client_id
     and w.event_name = b.event_name
     and w._source is distinct from 'capi_backfill'
     and w.event_time between timestamp_sub(b.event_time, interval 10 second)
                          and timestamp_add(b.event_time, interval 10 second)
    where b._source = 'capi_backfill'
      and b.client_id is not null
    group by b.client_id, b.event_name
{% endcall %}
