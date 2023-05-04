{{ config(materialized='incremental', unique_key='PK') }}

select
  PK,
  SK,
  CREATED_AT,
  to_timestamp_tz(to_varchar(UPDATED_AT)) as UPDATED_AT,
  POLICY_LOCATOR,
  STATUS
from dynamodb.prod_leads_table
where _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and (CREATED_AT > (select CREATED_AT from transformations_dynamodb.prod_leads order by CREATED_AT desc limit 1)
  or to_timestamp_tz(to_varchar(UPDATED_AT)) > (select UPDATED_AT from transformations_dynamodb.prod_leads order by UPDATED_AT desc limit 1))
{% endif %}