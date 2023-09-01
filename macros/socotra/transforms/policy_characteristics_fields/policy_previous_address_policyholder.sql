{% macro run_socotra_policy_prev_addr_ph(socotra_db, sf_schema) %}

select
	max(case when field_name = 'previous_street_address_policyholder' then field_value end) as previous_street_address_policyholder,
	max(case when field_name = 'previous_lot_unit_policyholder' then field_value end) as previous_lot_unit_policyholder,
	max(case when field_name = 'previous_city_policyholder' then field_value end) as previous_city_policyholder,
	max(case when field_name = 'previous_state_policyholder' then field_value end) as previous_state_policyholder,
	max(case when field_name = 'previous_zip_code_policyholder' then field_value end) as previous_zip_code_policyholder,
	max(case when field_name = 'previous_county_policyholder' then field_value end) as previous_county_policyholder,
	max(case when field_name = 'previous_country_policyholder' then field_value end) as previous_country_policyholder,
    pm.locator as policy_modification_locator,
    pm.policy_locator,
    poc.locator as policy_characteristics_locator,
    convert_timezone('America/New_York', to_timestamp_tz(poc.start_timestamp/1000)) as start_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.end_timestamp/1000)) as end_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.replaced_timestamp/1000)) as replaced_timestamp
from
	{{ socotra_db }}.policy_modification as pm
	left join {{ socotra_db }}.peril_characteristics as pc on pm.locator = pc.policy_modification_locator
	join {{ socotra_db }}.policy_characteristics as poc on poc.locator = pc.policy_characteristics_locator
	join {{ socotra_db }}.policy_characteristics_fields as pcf on pcf.policy_characteristics_locator = poc.locator
	where parent_name = 'previous_address_policyholder'
group by pm.locator, pm.policy_locator,  poc.locator, poc.start_timestamp, poc.end_timestamp, poc.datamart_created_timestamp, poc.datamart_updated_timestamp, poc.replaced_timestamp
{% endmacro %}
