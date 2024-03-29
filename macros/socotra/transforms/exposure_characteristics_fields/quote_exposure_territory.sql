{% macro run_socotra_quote_exposure_territory(socotra_db, sf_schema) %}

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
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'territory'
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
