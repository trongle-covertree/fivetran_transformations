{% macro run_socotra_policy_uw_details(socotra_db, sf_schema) %}

select
    max(case when field_name = 'uw_description' then field_value end) as uw_description,
    max(case when field_name = 'uw_manual' then field_value end) as uw_manual,
	policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
    	on pc.locator = pcf.policy_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as p
        on p.policy_characteristics_locator = pc.locator
	where parent_name = 'uw_details'
	{% if is_incremental() %}
		and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_uw_details order by datamart_created_timestamp desc limit 1)
			or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_uw_details order by datamart_updated_timestamp desc limit 1))
	{% endif %}
group by pcf.policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator
{% endmacro %}
