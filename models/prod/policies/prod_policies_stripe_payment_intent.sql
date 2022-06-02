{{ config(materialized='table') }}

select
  pk,
  sk,
  policy_locator,
  payment_intent_id
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'STRIPE_PAYMENT_INTENT') and _fivetran_deleted='FALSE'
