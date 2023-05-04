{{ config(materialized='incremental', unique_key='locator') }}

select
  PK,
  SK,
  IS_ELIGIBLE_FOR_INSURANCE,
  to_timestamp_tz(to_varchar(CREATED_TIMESTAMP)) as CREATED_TIMESTAMP,
  VERSION,
  to_timestamp_tz(to_varchar(UPDATED_TIMESTAMP)) as UPDATED_TIMESTAMP,
  LOCATOR,
  ENTITY,
  _fivetran_synced
from dynamodb.prod_socotra_policy_holder_table
where pk like 'POLICYHOLDER#%' and (sk like 'PERSON' or sk like 'LOCATOR') and _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and (to_timestamp_tz(to_varchar(CREATED_TIMESTAMP)) > (select CREATED_TIMESTAMP from transformations_dynamodb.prod_policyholders_person_locator order by CREATED_TIMESTAMP desc limit 1)
  or to_timestamp_tz(to_varchar(UPDATED_TIMESTAMP)) > (select UPDATED_TIMESTAMP from transformations_dynamodb.prod_policyholders_person_locator order by UPDATED_TIMESTAMP desc limit 1))
{% endif %}
