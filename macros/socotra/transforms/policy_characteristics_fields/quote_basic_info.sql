{% macro run_socotra_quote_basic_info(socotra_db, sf_schema) %}

select
	min(case when field_name = 'quote_inception_date' then field_value::date end) as quote_inception_date,
	min(case when field_name = 'date_of_birth' then field_value::date end) as date_of_birth,
	min(case when field_name = 'auto_policy_with_agency' then field_value::boolean end) as auto_policy_with_agency,
	quote_policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'policy_basic_info'
{% if is_incremental() %}
     and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_policy_basic_info order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_policy_basic_info order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.quote_policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
