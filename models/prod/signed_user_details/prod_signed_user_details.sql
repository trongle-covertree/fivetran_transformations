{{ config(materialized='incremental') }}

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
{% if is_incremental() %}
  and date > (select date from transformations_dynamodb.prod_signed_user_details order by date desc limit 1)
{% endif %}
