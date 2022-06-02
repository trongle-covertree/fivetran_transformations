{{ config(materialized='table') }}

select
  pk,
  sk,
  cancellation_category,
  effective_timestamp,
  documents,
  created_timestamp,
  title,
  policy_modification_locator,
  price,
  conflict_handling,
  name,
  issued_timestamp,
  policy_locator,
  state,
  locator,
  cancellation_comments,
  invoice_locator,
  reinstatement
from dynamodb_develop.deve_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'CANCELLATION%') and _fivetran_deleted='FALSE'
