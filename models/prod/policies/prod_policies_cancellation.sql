{{ config(materialized='table') }}

select
  pk,
  sk,
  iff(cancellation_category is null and title = 'Lapse', 'Lapse', cancellation_category) as cancellation_category,
  to_timestamp_ntz(to_number(effective_timestamp)/1000) as effective_timestamp,
  documents,
  to_timestamp_ntz(to_number(created_timestamp)/1000) as created_timestamp,
  title,
  policy_modification_locator,
  price,
  conflict_handling,
  name,
  to_timestamp_ntz(to_number(issued_timestamp)/1000) as issued_timestamp,
  policy_locator,
  state,
  locator,
  cancellation_comments,
  invoice_locator,
  reinstatement,
  _fivetran_synced
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'CANCELLATION%') and _fivetran_deleted='FALSE'
