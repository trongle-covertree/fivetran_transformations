{% macro run_socotra_quote_basic_info(socotra_db, sf_schema) %}

select
	max(case when field_name = 'quote_inception_date' then field_value::date end) as quote_inception_date,
	max(case when field_name = 'date_of_birth' then field_value::date end) as date_of_birth,
	max(case when field_name = 'auto_policy_with_agency' then field_value::boolean end) as auto_policy_with_agency,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'policy_basic_info'
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
