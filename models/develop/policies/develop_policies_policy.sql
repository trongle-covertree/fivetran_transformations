{{ config(materialized='table') }}

select
  pk,
  sk,
  documents,
  created_timestamp,
  issued_timestamp,
  fees,
  characteristics,
  config_version,
  gross_fees,
  payment_schedule_name,
  original_contract_start_timestamp,
  updated_timestamp,
  product_name,
  exposures,
  invoices,
  effective_contract_end_timestamp,
  product_locator,
  policyholder_locator,
  gross_fees_currency,
  original_contract_end_timestamp,
  display_id,
  modifications,
  locator,
  cancellation,
  currency,
  status,
  f.value::string as plan_type
from dynamodb_develop.deve_socotra_policy_table,
lateral flatten(deve_socotra_policy_table.exposures[0].characteristics[0].fieldGroupsByLocator, recursive=>true) f
where (pk like 'POLICY#%' and sk like 'POLICY') and _fivetran_deleted='FALSE' and f.path like '%personalized_plan_type[0]%'
