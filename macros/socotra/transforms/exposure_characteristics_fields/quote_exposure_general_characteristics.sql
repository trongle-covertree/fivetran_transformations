{% macro run_socotra_quote_exposure_gen_chars(socotra_db, sf_schema) %}

select
	max(case when field_name = 'usage' then field_value end) as "usage",
	max(case when field_name = 'unit_address' then field_value end) as unit_address,
	max(case when field_name = 'unit_level_suppl_uw' then field_value end) as unit_level_suppl_uw,
	max(case when field_name = 'inspections' then field_value end) as inspections,
	max(case when field_name = 'unit_details' then field_value end) as unit_details,
	max(case when field_name = 'additional_interest' then field_value end) as additional_interest,
	max(case when field_name = 'territory' then field_value end) as territory,
	max(case when field_name = 'unit_construction' then field_value end) as unit_construction,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name is null
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
