{% macro run_socotra_quote_exposure_unit_address(socotra_db, sf_schema) %}

select
	max(case when field_name = 'street_address' then field_value end) as street_address,
	max(case when field_name = 'lot_unit' then field_value end) as lot_unit,
	max(case when field_name = 'city' then field_value end) as city,
	max(case when field_name = 'state' then field_value end) as state,
	max(case when field_name = 'zip_code' then field_value end) as zip_code,
	max(case when field_name = 'county' then field_value end) as county,
	max(case when field_name = 'country' then field_value end) as country,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'unit_address'
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
