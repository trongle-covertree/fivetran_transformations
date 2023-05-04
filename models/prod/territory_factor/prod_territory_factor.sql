{{ config(materialized='incremental', unique_key='PK') }}

select
  PK,
  grid_id,
  primary_census_tract,
  primary_class,
  primary_county_fips,
  primary_county_name,
  primary_island,
  primary_place_name,
  primary_po_name,
  primary_ua_label,
  primary_ua_type,
  primary_zillow_community_name,
  primary_zip_code,
  state_fips,
  state_name,
  sub_region,
  territory_aop,
  territory_eq,
  territory_wh,
  uw_admitted
from dynamodb.prod_territory_factor_table
where _fivetran_deleted='FALSE'
{% if is_incremental() %}
  and PK not in (select PK from transformations_dynamodb.prod_territory_factor)
{% endif %}
