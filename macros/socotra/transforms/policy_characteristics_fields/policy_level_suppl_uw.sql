{% macro run_socotra_policy_level_suppl_uw(socotra_db, sf_schema) %}

select
	max(case when field_name = 'animal_bite' then field_value::boolean end) as animal_bite,
	max(case when field_name = 'conviction' then field_value::boolean end) as conviction,
	max(case when field_name = 'cancellation_renew' then field_value::boolean end) as cancellation_renew,
	max(case when field_name = 'reason_cancellation_renew' then field_value end) as reason_cancellation_renew,
	policy_characteristics_locator,
	policy_modification_locator,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
    	on pc.locator = pcf.policy_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as p
        on p.policy_characteristics_locator = pc.locator
	where parent_name = 'policy_level_suppl_uw'
{% if is_incremental() %}
    and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_level_suppl_uw order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_level_suppl_uw order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator
{% endmacro %}
