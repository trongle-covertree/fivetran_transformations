{% macro run_socotra_exposure_territory(socotra_db, sf_schema) %}

select
	max(case when field_name = 'loc_code' then field_value end) as loc_code,
	max(case when field_name = 'grid_id' then field_value::varchar end) as grid_id,
	max(case when field_name = 'territory_code_aop' then field_value end) as territory_code_aop,
	max(case when field_name = 'uw_admitted' then field_value end) as uw_admitted,
	max(case when field_name = 'long' then field_value::varchar end) as "LONG",
	max(case when field_name = 'lat' then field_value::varchar end) as lat,
	max(case when field_name = 'territory_code_windhail' then field_value end) as territory_code_windhail,
	max(case when field_name = 'territory_code_earthquake' then field_value end) as territory_code_earthquake,
	max(case when field_name = 'territory_code_floods' then field_value end) as territory_code_floods,
	max(case when field_name = 'territory_code_wild_fire' then field_value end) as territory_code_wild_fire,
	max(case when field_name = 'territory_code_hurricane' then field_value end) as territory_code_hurricane,
	max(case when field_name = 'county_fips' then field_value end) as county_fips,
	max(case when field_name = 'windpool' then field_value end) as windpool,
	max(case when field_name = 'distance_to_coast' then field_value end) as distance_to_coast,
	max(case when field_name = 'ct_mhcid' then field_value::varchar end) as ct_mhcid,
	exposure_locator,
	ecf.exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	to_timestamp_tz(ec.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ec.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as pc
		on ec.locator = pc.exposure_characteristics_locator
	where parent_name = 'territory'
{% if is_incremental() %}
    and (to_timestamp_tz(ec.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_territory order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ec.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_territory order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, policy_mod
{% endmacro %}
