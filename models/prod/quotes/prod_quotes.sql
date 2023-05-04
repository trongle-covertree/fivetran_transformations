{{ config(materialized='incremental', unique_key='quote_locator') }}

select
  PK,
  SK,
  to_timestamp_tz(to_varchar(CREATED_AT)) as CREATED_AT,
  DESCRIPTION,
  POLICY_LOCATOR,
  QUOTE_LOCATOR,
  STATUS,
  to_timestamp_tz(to_varchar(UPDATED_AT)) as UPDATED_AT,
  CONVERTED
from dynamodb.prod_socotra_quote_table
where (pk like 'QUOTE#%' and sk like 'POLICY#%') and _fivetran_deleted='FALSE' and
  lower(sk) not in ('', 'policy#101255772', 'policy#100599038', 'policy#100640796', 'policy#100712180', 'policy#100825636', 'policy#100825766', 'policy#100825954',
  'policy#100826086', 'policy#100849710', 'policy#100902420', 'policy#100902972', 'policy#100903084', 'policy#100931144', 'policy#100931232', 'policy#100957670',
  'policy#100979166', 'policy#101178036', 'policy#101132926', 'policy#101298090', 'policy#101298206', 'policy#101298618', 'policy#101298854', 'policy#101299186',
  'policy#101436546', 'policy#101468294', 'policy#101468958')
{% if is_incremental() %}
  and (to_timestamp_tz(to_varchar(CREATED_AT)) > (select CREATED_AT from transformations_dynamodb.prod_quotes order by CREATED_AT desc limit 1)
  or to_timestamp_tz(to_varchar(UPDATED_AT)) > (select UPDATED_AT from transformations_dynamodb.prod_quotes order by UPDATED_AT desc limit 1))
{% endif %}
