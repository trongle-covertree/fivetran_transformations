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
	to_timestamp_tz(ec.start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(ec.end_timestamp/1000) as end_timestamp,
	to_timestamp_tz(ec.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ec.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	where parent_name is null
{% if is_incremental() %}
    and (to_timestamp_tz(ec.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_general_characteristics order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ec.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_general_characteristics order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, ec.start_timestamp, ec.end_timestamp
{% endmacro %}
