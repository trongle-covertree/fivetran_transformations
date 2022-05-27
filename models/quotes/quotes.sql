{{ config(materialized='table') }}

select
  PK,
  SK,
  CREATED_AT,
  DESCRIPTION,
  POLICY_LOCATOR,
  QUOTE_LOCATOR,
  STATUS,
  UPDATED_AT,
  CONVERTED
from dynamodb_develop.deve_socotra_policy_table
where (pk like 'QUOTE#%' and sk like 'POLICY#%') and _fivetran_deleted='FALSE'