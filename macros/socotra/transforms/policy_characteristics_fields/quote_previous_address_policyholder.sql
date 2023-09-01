{% macro run_socotra_quote_prev_addr_ph(socotra_db, sf_schema) %}

select
	max(case when field_name = 'previous_street_address_policyholder' then field_value end) as previous_street_address_policyholder,
	max(case when field_name = 'previous_lot_unit_policyholder' then field_value end) as previous_lot_unit_policyholder,
	max(case when field_name = 'previous_city_policyholder' then field_value end) as previous_city_policyholder,
	max(case when field_name = 'previous_state_policyholder' then field_value end) as previous_state_policyholder,
	max(case when field_name = 'previous_zip_code_policyholder' then field_value end) as previous_zip_code_policyholder,
	max(case when field_name = 'previous_county_policyholder' then field_value end) as previous_county_policyholder,
	max(case when field_name = 'previous_country_policyholder' then field_value end) as previous_country_policyholder,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'previous_address_policyholder'
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
