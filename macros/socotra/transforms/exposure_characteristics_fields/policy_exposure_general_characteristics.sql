{% macro run_socotra_exposure_gen_chars(socotra_db, sf_schema) %}

select
	max(case when field_name = 'usage' then field_value end) as "usage",
	max(case when field_name = 'unit_address' then field_value end) as unit_address,
	max(case when field_name = 'unit_level_suppl_uw' then field_value end) as unit_level_suppl_uw,
	max(case when field_name = 'inspections' then field_value end) as inspections,
	max(case when field_name = 'unit_details' then field_value end) as unit_details,
	max(case when field_name = 'additional_interest' then field_value end) as additional_interest,
	max(case when field_name = 'territory' then field_value end) as territory,
	max(case when field_name = 'unit_construction' then field_value end) as unit_construction,
	exposure_locator,
	ecf.exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	convert_timezone('America/New_York', to_timestamp_tz(ec.start_timestamp/1000)) as start_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(ec.end_timestamp/1000)) as end_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as pc
		on ec.locator = pc.exposure_characteristics_locator
	where parent_name is null
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, policy_modification_locator, ec.start_timestamp, ec.end_timestamp
{% endmacro %}
