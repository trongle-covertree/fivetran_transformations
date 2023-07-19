{{ config(materialized='table',
    post_hook = "{{ run_non_covertree_policy_update('transformations_dynamodb','prod' ) }}") }}

(select
  pk,
  sk,
  documents,
  to_timestamp_ntz(to_number(created_timestamp)/1000) as created_timestamp,
  to_timestamp_ntz(to_number(created_timestamp_dynamo)/1000) as created_timestamp_dynamo,
  to_timestamp_ntz(to_number(issued_timestamp)/1000) as issued_timestamp,
  fees,
  characteristics,
  config_version,
  gross_fees,
  payment_schedule_name,
  to_timestamp_ntz(original_contract_start_timestamp/1000) as original_contract_start_timestamp,
  to_timestamp_ntz(to_number(updated_timestamp)/1000) as updated_timestamp,
  product_name,
  exposures,
  invoices,
  to_timestamp_ntz(effective_contract_end_timestamp/1000) as effective_contract_end_timestamp,
  product_locator,
  policyholder_locator,
  gross_fees_currency,
  to_timestamp_ntz(original_contract_end_timestamp/1000) as original_contract_end_timestamp,
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
  '101255196', '101227402', '101290498', '101291026', '101291182', '101291550', '101291884', '101212396',
  '101436546', '101468294', '101468958', '101661042', '101633700', '101634302', '101729200', '101729706',
  '101750784', '101785484', '101825810', '101825884', '101767248', '101942576', '101942446', '101942812',
  '101942770', '102413580', '102414276'))
union
(select deal_id, null, null, property_createdate, property_closedate, null, null, null, null, null, property_policy_effective_date, null, null, null, null, property_policy_end_date,
  null, null, null, null, null, null, property_policy_number, null, null, iff(property_policy_end_date > current_timestamp(), 'Policy-Activated', 'Policy-Cancelled'),
  null, null from transformations_dynamodb.prod_non_covertree_policies)
