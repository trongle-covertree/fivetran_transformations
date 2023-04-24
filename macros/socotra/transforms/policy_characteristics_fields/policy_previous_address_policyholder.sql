{% macro run_socotra_policy_prev_addr_ph(socotra_db, sf_schema) %}

select
	max(case when field_name = 'previous_street_address_policyholder' then field_value end) as previous_street_address_policyholder,
	max(case when field_name = 'previous_lot_unit_policyholder' then field_value end) as previous_lot_unit_policyholder,
	max(case when field_name = 'previous_city_policyholder' then field_value end) as previous_city_policyholder,
	max(case when field_name = 'previous_state_policyholder' then field_value end) as previous_state_policyholder,
	max(case when field_name = 'previous_zip_code_policyholder' then field_value end) as previous_zip_code_policyholder,
	max(case when field_name = 'previous_county_policyholder' then field_value end) as previous_county_policyholder,
	max(case when field_name = 'previous_country_policyholder' then field_value end) as previous_country_policyholder,
	pcf.policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	to_timestamp_tz(pc.start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(pc.end_timestamp/1000) as end_timestamp,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
    	on pc.locator = pcf.policy_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as p
        on p.policy_characteristics_locator = pc.locator
	where parent_name = 'previous_address_policyholder'
{% if is_incremental() %}
    and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_previous_address_policyholder order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_previous_address_policyholder order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator, pc.start_timestamp, pc.end_timestamp
{% endmacro %}
