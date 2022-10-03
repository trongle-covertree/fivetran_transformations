{{ config(materialized='table') }}

select
  PK,
  SK,
  IS_ELIGIBLE_FOR_INSURANCE,
  to_timestamp_ltz(to_varchar(CREATED_TIMESTAMP)) as CREATED_TIMESTAMP,
  VERSION,
  to_timestamp_ltz(to_varchar(UPDATED_TIMESTAMP)) as UPDATED_TIMESTAMP,
  LOCATOR,
  ENTITY,
  _fivetran_synced
from dynamodb_develop.deve_socotra_policy_holder_table
where pk like 'POLICYHOLDER#%' and (sk like 'PERSON' or sk like 'LOCATOR') and _fivetran_deleted='FALSE'
