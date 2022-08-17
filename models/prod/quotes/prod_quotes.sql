{{ config(materialized='table') }}

select
  PK,
  SK,
  CREATED_AT,
  DESCRIPTION,
  POLICY_LOCATOR::string,
  QUOTE_LOCATOR::string,
  STATUS,
  UPDATED_AT,
  CONVERTED
from dynamodb.prod_socotra_quote_table
where (pk like 'QUOTE#%' and sk like 'POLICY#%') and _fivetran_deleted='FALSE'
