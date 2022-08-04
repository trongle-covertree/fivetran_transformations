{{ config(materialized='table') }}

select
  {{ dbt_utils.surrogate_key(
    'pk',
    'created_timestamp'
  )}} as policy_id,
  pk,
  sk,
  documents,
  {{ dbt_date.from_unixtimestamp("created_timestamp") }} as created_timestamp,
  {{ dbt_date.from_unixtimestamp("issued_timestamp") }} as issued_timestamp,
  fees,
  characteristics,
  config_version,
  gross_fees,
  payment_schedule_name,
  {{ dbt_date.from_unixtimestamp("original_contract_start_timestamp") }} as original_contract_start_timestamp,
  {{ dbt_date.from_unixtimestamp("updated_timestamp") }} as updated_timestamp,
  product_name,
  exposures,
  invoices,
  {{ dbt_date.from_unixtimestamp("effective_contract_end_timestamp") }} as effective_contract_end_timestamp,
  product_locator,
  policyholder_locator,
  gross_fees_currency,
  {{ dbt_date.from_unixtimestamp("original_contract_end_timestamp") }} as original_contract_end_timestamp,
  display_id,
  modifications,
  locator,
  cancellation,
  currency,
  status,
  _fivetran_synced
from dynamodb_develop.deve_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'POLICY') and _fivetran_deleted='FALSE'
