{% macro run_socotra_quote_level_suppl_uw(socotra_db, sf_schema) %}

select
	min(case when field_name = 'animal_bite' then field_value::boolean end) as animal_bite,
	min(case when field_name = 'conviction' then field_value::boolean end) as conviction,
	min(case when field_name = 'cancellation_renew' then field_value::boolean end) as cancellation_renew,
	min(case when field_name = 'reason_cancellation_renew' then field_value end) as reason_cancellation_renew,
	quote_policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'policy_level_suppl_uw'
{% if is_incremental() %}
    and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_policy_level_suppl_uw order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_policy_level_suppl_uw order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.quote_policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
