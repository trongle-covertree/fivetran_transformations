{% macro run_socotra_quote_ad_insured(socotra_db, sf_schema) %}

select
	max(case when field_name = 'additionalinsured_date_of_birth' then field_value::date end) as additionalinsured_date_of_birth,
	max(case when field_name = 'ad_first_name' then field_value end) as ad_first_name,
	max(case when field_name = 'ad_last_name' then field_value end) as ad_last_name,
	max(case when field_name = 'relationship_to_policyholder' then field_value end) as relationship_to_policyholder,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'ad_insured'
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
