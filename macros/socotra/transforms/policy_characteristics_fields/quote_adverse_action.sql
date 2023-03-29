{% macro run_socotra_quote_adverse_action(socotra_db, sf_schema) %}

select
	min(case when field_name = 'reason_description' then field_value end) as reason_description,
	min(case when field_name = 'reason_code' then field_value end) as reason_code,
	quote_policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'adverse_action'
{% if is_incremental() %}
    and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema}}.quote_policy_adverse_action order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema}}.quote_policy_adverse_action order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.quote_policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
