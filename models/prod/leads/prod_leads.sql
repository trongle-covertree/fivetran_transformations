{{ config(materialized='table') }}

select
  PK,
  SK,
  CREATED_AT,
  to_timestamp_tz(to_varchar(UPDATED_AT)) as UPDATED_AT,
  POLICY_LOCATOR,
  STATUS,
  PARTNER_KEY,
  CREDIT,
  TREMENDOUS_ORDER_ID
from dynamodb.prod_leads_table
where _fivetran_deleted='FALSE'
