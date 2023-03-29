{% macro run_socotra_quote_uw_details(socotra_db, sf_schema) %}

select
    min(case when field_name = 'uw_description' then field_value end) as uw_description,
    min(case when field_name = 'uw_manual' then field_value end) as uw_manual,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'uw_details'
	{% if is_incremental() %}
		and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_uw_details order by datamart_created_timestamp desc limit 1)
			or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_uw_details order by datamart_updated_timestamp desc limit 1))
	{% endif %}
group by pcf.quote_policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
