

{{ config(materialized='incremental', unique_key='ID') }}

select
  PK,
  SK,
  ID
from dynamodb.prod_socotra_policy_holder_table
where (pk like 'POLICYHOLDER%' and pk not like 'POLICYHOLDER#%') and sk like 'STRIPE_CUSTOMER' and _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and ID not in (select ID from transformations_dynamodb.prod_policyholders_stripe_customer)
{% endif %}
