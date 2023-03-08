{% macro run_socotra_exposure_inspections(socotra_db, sf_schema) %}

select
	min(case when field_name = 'interior' then field_value::boolean end) as interior,
	min(case when field_name = 'exterior' then field_value::boolean end) as exterior,
	min(case when field_name = 'aerial' then field_value::boolean end) as aerial,
	exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ecf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ecf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	where parent_name = 'inspections'
{% if is_incremental() %}
    and (to_timestamp_tz(ecf.datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_inspections order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ecf.datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_inspections order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by ecf.exposure_characteristics_locator, ecf.datamart_created_timestamp, ecf.datamart_updated_timestamp, ec.policy_locator
{% endmacro %}
