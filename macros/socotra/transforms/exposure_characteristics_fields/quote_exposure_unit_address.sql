{% macro run_socotra_quote_exposure_unit_address(socotra_db, sf_schema) %}

select
	min(case when field_name = 'street_address' then field_value end) as street_address,
	min(case when field_name = 'lot_unit' then field_value end) as lot_unit,
	min(case when field_name = 'city' then field_value end) as city,
	min(case when field_name = 'state' then field_value end) as state,
	min(case when field_name = 'zip_code' then field_value end) as zip_code,
	min(case when field_name = 'county' then field_value end) as county,
	min(case when field_name = 'country' then field_value end) as country,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'unit_address'
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
