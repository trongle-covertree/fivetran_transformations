{{ config(materialized='table') }}

select
  PK,
  SK,
  IS_ELIGIBLE_FOR_INSURANCE,
  CREATED_TIMESTAMP,
  VERSION,
  UPDATED_TIMESTAMP,
  LOCATOR,
  ENTITY
from dynamodb_develop.deve_socotra_policy_holder_table
where pk like 'POLICYHOLDER#%' and (sk like 'PERSON' or sk like 'LOCATOR') and _fivetran_deleted='FALSE'
