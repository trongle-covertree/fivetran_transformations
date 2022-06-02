{{ config(materialized='table') }}

select
  pk,
  sk,
  policy_locator
from dynamodb.prod_socotra_policy_table
where (pk like 'ADDRESS_HASH#%' and sk like 'POLICY#%') and _fivetran_deleted='FALSE'
