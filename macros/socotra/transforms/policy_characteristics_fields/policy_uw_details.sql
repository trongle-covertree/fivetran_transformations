{% macro run_socotra_policy_uw_details(socotra_db, sf_schema) %}

select
    min(case when field_name = 'uw_description' then field_value end) as uw_description,
    min(case when field_name = 'uw_manual' then field_value end) as uw_manual,
	policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
    	on pc.locator = pcf.policy_characteristics_locator
	where parent_name = 'uw_details'
	{% if is_incremental() %}
		and (to_timestamp_tz(pcf.datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.policy_uw_details order by datamart_created_timestamp desc limit 1)
			or to_timestamp_tz(pcf.datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_uw_details order by datamart_updated_timestamp desc limit 1))
	{% endif %}
group by pcf.policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
