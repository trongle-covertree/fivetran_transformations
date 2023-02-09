{{ config(materialized='incremental') }}

select
  pk,
  sk,
  documents,
  {{ dbt_date.from_unixtimestamp("created_timestamp", format="milliseconds") }} as created_timestamp,
  {{ dbt_date.from_unixtimestamp("issued_timestamp", format="milliseconds") }} as issued_timestamp,
  fees,
  characteristics,
  config_version,
  gross_fees,
  payment_schedule_name,
  {{ dbt_date.from_unixtimestamp("original_contract_start_timestamp", format="milliseconds") }} as original_contract_start_timestamp,
  {{ dbt_date.from_unixtimestamp("updated_timestamp", format="milliseconds") }} as updated_timestamp,
  product_name,
  exposures,
  invoices,
  {{ dbt_date.from_unixtimestamp("effective_contract_end_timestamp", format="milliseconds") }} as effective_contract_end_timestamp,
  product_locator,
  policyholder_locator,
  gross_fees_currency,
  {{ dbt_date.from_unixtimestamp("original_contract_end_timestamp", format="milliseconds") }} as original_contract_end_timestamp,
  display_id,
  modifications,
  locator,
  cancellation,
  currency,
  status,
  _fivetran_synced,
  quote_locator
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'POLICY') and _fivetran_deleted='FALSE' and
  locator not in ('101255772', '100599038', '100640796', '100712180', '100825636', '100825766', '100825954',
  '100826086', '100849710', '100902420', '100902972', '100903084', '100931144', '100931232', '100957670',
  '100979166', '101178036', '101132926', '101298090', '101298206', '101298618', '101298854', '101299186',
  '101255196', '101227402', '101290498', '101291026', '101291182', '101291550', '101291884', '101212396')
{% if is_incremental() %}
  and ({{ dbt_date.from_unixtimestamp("created_timestamp", format="milliseconds") }} as created_timestamp > (select max(created_timestamp) from {{ this }}) or {{ dbt_date.from_unixtimestamp("updated_timestamp", format="milliseconds") }} as updated_timestamp > (select max(updated_timestamp) from {{ this }}))
{% endif %}
