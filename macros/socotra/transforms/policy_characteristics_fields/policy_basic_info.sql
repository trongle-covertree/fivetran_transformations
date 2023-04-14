{% macro run_socotra_policy_basic_info(socotra_db, sf_schema) %}

select
	max(case when field_name = 'quote_inception_date' then field_value::date end) as quote_inception_date,
	max(case when field_name = 'date_of_birth' then field_value::date end) as date_of_birth,
	max(case when field_name = 'auto_policy_with_agency' then field_value::boolean end) as auto_policy_with_agency,
	pcf.policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
    	on pc.locator = pcf.policy_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as p
        on p.policy_characteristics_locator = pc.locator
	where parent_name = 'policy_basic_info'
{% if is_incremental() %}
     and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_basic_info order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_basic_info order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator
{% endmacro %}
