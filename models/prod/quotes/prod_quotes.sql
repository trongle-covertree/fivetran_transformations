{{ config(materialized='table') }}

select
  PK,
  SK,
  {{ dbt_date.from_unixtimestamp("created_at", format="milliseconds") }} as CREATED_AT,
  DESCRIPTION,
  POLICY_LOCATOR,
  QUOTE_LOCATOR,
  STATUS,
  {{ dbt_date.from_unixtimestamp("updated_at", format="milliseconds") }} as UPDATED_AT,
  CONVERTED
from dynamodb.prod_socotra_quote_table
where (pk like 'QUOTE#%' and sk like 'POLICY#%') and _fivetran_deleted='FALSE'
