{{ config(materialized='incremental', unique_key='locator') }}

select
  pk,
  sk,
  automated_underwriting_result,
  config_version,
  convert_timezone('America/New_York', to_timestamp_ntz(to_number(created_timestamp)/1000)) as created_timestamp,
  display_id,
  convert_timezone('America/New_York', to_timestamp_ntz(effective_timestamp/1000)) as effective_timestamp,
  exposure_modifications,
  field_groups_by_locator,
  field_values,
  convert_timezone('America/New_York', to_timestamp_ntz(to_number(issued_timestamp)/1000)) as issued_timestamp,
  locator,
  media_by_locator,
  name,
  new_policy_characteristics_locator,
  new_policy_characteristics_locators,
  number,
  convert_timezone('America/New_York', to_timestamp_ntz(policy_end_timestamp/1000)) as policy_end_timestamp,
  policyholder_locator,
  policy_locator,
  premium_change,
  premium_change_currency,
  product_locator,
  convert_timezone('America/New_York', to_timestamp_ntz(to_number(updated_timestamp)/1000)) as updated_timestamp
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'MODIFICATION#%') and _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and (convert_timezone('America/New_York', to_timestamp_ntz(to_number(created_timestamp)/1000)) > (select created_timestamp from transformations_dynamodb.prod_policies_modifications order by created_timestamp desc limit 1)
    or convert_timezone('America/New_York', to_timestamp_ntz(updated_timestamp/1000)) > (select updated_timestamp from transformations_dynamodb.prod_policies_modifications order by updated_timestamp desc limit 1))
{% endif %}
