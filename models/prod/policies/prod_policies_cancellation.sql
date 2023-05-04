{{ config(materialized='incremental', unique_key='locator') }}

select
  pk,
  sk,
  iff(cancellation_category is null and title = 'Lapse', 'Lapse', cancellation_category) as cancellation_category,
  to_timestamp_tz(to_varchar(effective_timestamp)) as effective_timestamp,
  documents,
  to_timestamp_tz(to_varchar(created_timestamp)) as created_timestamp,
  title,
  policy_modification_locator,
  price,
  conflict_handling,
  name,
  to_timestamp_tz(to_varchar(issued_timestamp)) as issued_timestamp,
  policy_locator,
  state,
  locator,
  cancellation_comments,
  invoice_locator,
  reinstatement,
  _fivetran_synced
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'CANCELLATION%') and _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and to_timestamp_tz(to_varchar(created_timestamp)) > (select created_timestamp from transformations_dynamodb.prod_policies_cancellation order by created_timestamp desc limit 1)
{% endif %}
