{{ config(materialized='table') }}

select pk, peril_locator, coverage, name, value, spp_desc, spp_type, spp_value, max(created_timestamp) as created_timestamp
from prod_policy_exposures_perils_values
group by pk, peril_locator, coverage, name, value, spp_desc, spp_type, spp_value
order by created_timestamp desc