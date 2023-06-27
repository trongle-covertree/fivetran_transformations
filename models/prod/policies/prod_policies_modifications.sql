{{ config(materialized='table') }}

select
  pk,
  sk,
  automated_underwriting_result,
  cancellation_name,
  config_version,
  convert_timezone('America/New_York', to_timestamp_ntz(to_number(created_timestamp)/1000)) as created_timestamp,
  display_id,
  convert_timezone('America/New_York', to_timestamp_ntz(effective_timestamp/1000)) as effective_timestamp,
  endorsement_locator,
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
  pricing,
  product_locator,
  renewal_locator,
  convert_timezone('America/New_York', to_timestamp_ntz(to_number(updated_timestamp)/1000)) as updated_timestamp
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'MODIFICATION#%') and _fivetran_deleted='FALSE'
