{{ config(materialized='table') }}

select
  pk,
  sk,
  pricing
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'DEFAULT_COVERAGES') and _fivetran_deleted='FALSE'
