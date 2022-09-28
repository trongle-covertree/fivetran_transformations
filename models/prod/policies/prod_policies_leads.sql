{{ config(materialized='table') }}

select
  pk,
  sk,
  {{ dbt_date.from_unixtimestamp("updated_at", format="milliseconds") }} as updated_at,
  partner_name,
  partner_key,
  lead_id
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'LEAD') and _fivetran_deleted='FALSE'
