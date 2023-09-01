{% macro run_socotra_quote_level_suppl_uw(socotra_db, sf_schema) %}

select
	max(case when field_name = 'animal_bite' then field_value::boolean end) as animal_bite,
	max(case when field_name = 'conviction' then field_value::boolean end) as conviction,
	max(case when field_name = 'cancellation_renew' then field_value::boolean end) as cancellation_renew,
	max(case when field_name = 'reason_cancellation_renew' then field_value end) as reason_cancellation_renew,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'policy_level_suppl_uw'
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
