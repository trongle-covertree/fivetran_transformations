{{ config(materialized='table') }}

select
  pk,
  sk,
  to_timestamp_ntz(to_varchar(updated_at), 3) as updated_at,
  partner_name,
  partner_key,
  lead_id
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'LEAD') and _fivetran_deleted='FALSE'
