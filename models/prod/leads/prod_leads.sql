{{ config(materialized='table') }}

select
  PK,
  SK,
  CREATED_AT,
  UPDATED_AT,
  POLICY_LOCATOR,
  STATUS
from dynamodb.prod_leads_table
where _fivetran_deleted='FALSE'
