{% macro run_socotra_quote_exposure_unit_address(socotra_db, sf_schema) %}

select
	min(case when field_name = 'street_address' then field_value end) as street_address,
	min(case when field_name = 'lot_unit' then field_value end) as lot_unit,
	min(case when field_name = 'city' then field_value end) as city,
	min(case when field_name = 'state' then field_value end) as state,
	min(case when field_name = 'zip_code' then field_value end) as zip_code,
	min(case when field_name = 'county' then field_value end) as county,
	min(case when field_name = 'country' then field_value end) as country,
	quote_exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	ecf.datamart_created_timestamp,
	ecf.datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'unit_address'
{% if is_incremental() %}
    and (ecf.datamart_created_timestamp > (select datamart_created_timestamp from {{ sf_schema }}.quote_exposure_unit_address order by datamart_created_timestamp desc limit 1)
      or ecf.datamart_updated_timestamp > (select datamart_updated_timestamp from {{ sf_schema }}.quote_exposure_unit_address order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by ecf.quote_exposure_characteristics_locator, ecf.datamart_created_timestamp, ecf.datamart_updated_timestamp, ec.policy_locator
{% endmacro %}
