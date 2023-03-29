{% macro run_socotra_quote_exposure_gen_chars(socotra_db, sf_schema) %}

select
	min(case when field_name = 'usage' then field_value end) as "usage",
	min(case when field_name = 'unit_address' then field_value end) as unit_address,
	min(case when field_name = 'unit_level_suppl_uw' then field_value end) as unit_level_suppl_uw,
	min(case when field_name = 'inspections' then field_value end) as inspections,
	min(case when field_name = 'unit_details' then field_value end) as unit_details,
	min(case when field_name = 'additional_interest' then field_value end) as additional_interest,
	min(case when field_name = 'territory' then field_value end) as territory,
	min(case when field_name = 'unit_construction' then field_value end) as unit_construction,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ecf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ecf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name is null
{% if is_incremental() %}
    and (to_timestamp_tz(ecf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_exposure_general_characteristics order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ecf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_exposure_general_characteristics order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ecf.datamart_created_timestamp, ecf.datamart_updated_timestamp, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
