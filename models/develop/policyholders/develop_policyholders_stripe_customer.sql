

{{ config(materialized='table') }}

select
  PK,
  SK,
  ID
from dynamodb_develop.deve_socotra_policy_holder_table
where (pk like 'POLICYHOLDER%' and pk not like 'POLICYHOLDER#%') and sk like 'STRIPE_CUSTOMER' and _fivetran_deleted='FALSE'
