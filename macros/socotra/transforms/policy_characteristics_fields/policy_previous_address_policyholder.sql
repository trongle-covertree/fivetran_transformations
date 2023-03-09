{% macro run_socotra_policy_prev_addr_ph(socotra_db, sf_schema) %}

select
	min(case when field_name = 'previous_street_address_policyholder' then field_value end) as previous_street_address_policyholder,
	min(case when field_name = 'previous_lot_unit_policyholder' then field_value end) as previous_lot_unit_policyholder,
	min(case when field_name = 'previous_city_policyholder' then field_value end) as previous_city_policyholder,
	min(case when field_name = 'previous_state_policyholder' then field_value end) as previous_state_policyholder,
	min(case when field_name = 'previous_zip_code_policyholder' then field_value end) as previous_zip_code_policyholder,
	min(case when field_name = 'previous_county_policyholder' then field_value end) as previous_county_policyholder,
	min(case when field_name = 'previous_country_policyholder' then field_value end) as previous_country_policyholder,
	policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
    	on pc.locator = pcf.policy_characteristics_locator
	where parent_name = 'previous_address_policyholder'
{% if is_incremental() %}
    and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_previous_address_policyholder order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_previous_address_policyholder order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
