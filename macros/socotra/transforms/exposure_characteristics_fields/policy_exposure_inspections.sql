{% macro run_socotra_exposure_inspections(socotra_db, sf_schema) %}

select
	max(case when field_name = 'interior' then field_value::boolean end) as interior,
	max(case when field_name = 'exterior' then field_value::boolean end) as exterior,
	max(case when field_name = 'aerial' then field_value::boolean end) as aerial,
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
	where parent_name = 'inspections'
{% if is_incremental() %}
    and (to_timestamp_tz(ec.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_inspections order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ec.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_inspections order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, ec.start_timestamp, ec.end_timestamp
{% endmacro %}
