{{ config(materialized='incremental', unique_key='lead_id') }}

select
  pk,
  sk,
  to_timestamp_tz(to_varchar(updated_at)) as updated_at,
  partner_name,
  partner_key,
  lead_id
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'LEAD') and _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and to_timestamp_tz(to_varchar(updated_at)) > (select updated_at from transformations_dynamodb.prod_policies_leads order by updated_at desc limit 1)
{% endif %}
