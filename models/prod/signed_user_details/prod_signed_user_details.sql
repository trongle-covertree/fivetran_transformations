{{ config(materialized='table') }}

select
  PK,
  SK,
  DATE,
  RAW_HEADERS,
  SIGNED_NAME,
  OS,
  BROWSER,
  IP,
  DEVICE,
  POLICY_LOCATOR
from dynamodb.prod_signed_user_details
where _fivetran_deleted='FALSE'
