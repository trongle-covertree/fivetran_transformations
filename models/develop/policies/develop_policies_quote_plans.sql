{{ config(materialized='table') }}

select
  pk,
  sk,
  silver_quote_locator,
  silver_quote_data,
  silver_quote_pricing,
  gold_quote_locator,
  gold_quote_data,
  gold_quote_pricing,
  platinum_quote_locator,
  platinum_quote_data,
  platinum_quote_pricing,
  customization,
  descriptions
from dynamodb_develop.deve_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'QUOTE_PLANS') and _fivetran_deleted='FALSE'
