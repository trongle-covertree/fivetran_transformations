{{ config(materialized='table') }}

select
  PK,
  SK,
  CREATED_AT,
  UPDATED_AT,
  POLICY_LOCATOR,
  STATUS
from dynamodb_develop.deve_leads_table
where _fivetran_deleted='FALSE'