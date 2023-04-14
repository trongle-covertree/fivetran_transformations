{% macro run_socotra_exposure_usage(socotra_db, sf_schema) %}

select
	max(case when field_name = 'policy_usage' then field_value end) as policy_usage,
	max(case when field_name = 'vacancy_reason' then field_value end) as vacancy_reason,
	max(case when field_name = 'short_term_rental_surcharge' then field_value::boolean end) as short_term_rental_surcharge,
	exposure_locator,
	ecf.exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	to_timestamp_tz(ec.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ec.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as pc
		on ec.locator = pc.exposure_characteristics_locator
	where parent_name = 'usage'
{% if is_incremental() %}
    and (to_timestamp_tz(ec.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_usage order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ec.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_usage order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, policy_modification_locator
{% endmacro %}
