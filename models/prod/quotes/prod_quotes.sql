{{ config(materialized='table') }}

select
  PK,
  SK,
  CREATED_AT,
  DESCRIPTION,
  POLICY_LOCATOR,
  QUOTE_LOCATOR,
  STATUS,
  UPDATED_AT,
  CONVERTED
from dynamodb.prod_socotra_quote_table
where (pk like 'QUOTE#%' and sk like 'POLICY#%') and _fivetran_deleted='FALSE' and
  lower(sk) not in ('', 'policy#101255772', 'policy#100599038', 'policy#100640796', 'policy#100712180', 'policy#100825636', 'policy#100825766', 'policy#100825954',
  'policy#100826086', 'policy#100849710', 'policy#100902420', 'policy#100902972', 'policy#100903084', 'policy#100931144', 'policy#100931232', 'policy#100957670',
  'policy#100979166', 'policy#101178036', 'policy#101132926')
