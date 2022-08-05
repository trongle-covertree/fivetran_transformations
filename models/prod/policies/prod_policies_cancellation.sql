{{ config(materialized='table') }}

select
  pk,
  sk,
  cancellation_category,
  {{ dbt_date.from_unixtimestamp("effective_timestamp", format="milliseconds") }} as effective_timestamp,
  documents,
  {{ dbt_date.from_unixtimestamp("created_timestamp", format="milliseconds") }} as created_timestamp,
  title,
  policy_modification_locator,
  price,
  conflict_handling,
  name,
  {{ dbt_date.from_unixtimestamp("issued_timestamp", format="milliseconds") }} as issued_timestamp,
  policy_locator,
  state,
  locator,
  cancellation_comments,
  invoice_locator,
  reinstatement
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'CANCELLATION%') and _fivetran_deleted='FALSE'
