{{ config(materialized='incremental', unique_key='sk') }}

select
  pk,
  sk,
  documents,
  to_timestamp_tz(to_varchar(created_timestamp)) as created_timestamp,
  to_timestamp_tz(to_varchar(issued_timestamp)) as issue_timestamp,
  fees,
  characteristics,
  config_version,
  gross_fees,
  payment_schedule_name,
  to_timestamp_tz(to_varchar(original_contract_start_timestamp)) as original_contract_start_timestamp,
  to_timestamp_tz(to_varchar(updated_timestamp)) as updated_timestamp,
  product_name,
  exposures,
  invoices,
  to_timestamp_tz(to_varchar(effective_contract_end_timestamp)) as effective_contract_end_timestamp,
  product_locator,
  policyholder_locator,
  gross_fees_currency,
  to_timestamp_tz(to_varchar(original_contract_end_timestamp)) as original_contract_end_timestamp,
  display_id,
  modifications,
  locator,
  cancellation,
  currency
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICYHOLDER#%' and sk like 'POLICY#%') and _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and (to_timestamp_tz(to_varchar(created_timestamp)) > (select created_timestamp from transformations_dynamodb.prod_policies_policyholder_pk_policy_number_sk order by created_timestamp desc limit 1)
  or to_timestamp_tz(to_varchar(updated_timestamp)) > (select updated_timestamp from transformations_dynamodb.prod_policies_policyholder_pk_policy_number_sk order by updated_timestamp desc limit 1))
{% endif %}
