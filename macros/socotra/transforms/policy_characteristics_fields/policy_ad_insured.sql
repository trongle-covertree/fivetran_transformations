{% macro run_socotra_ad_insured(socotra_db, sf_schema) %}

select
	max(case when field_name = 'additionalinsured_date_of_birth' then field_value::date end) as additionalinsured_date_of_birth,
	max(case when field_name = 'ad_first_name' then field_value end) as ad_first_name,
	max(case when field_name = 'ad_last_name' then field_value end) as ad_last_name,
	max(case when field_name = 'relationship_to_policyholder' then field_value end) as relationship_to_policyholder,
    pm.locator as policy_modification_locator,
    pm.policy_locator,
    poc.locator as policy_characteristics_locator,
    convert_timezone('America/New_York', to_timestamp_tz(poc.start_timestamp/1000)) as start_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.end_timestamp/1000)) as end_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.replaced_timestamp/1000)) as replaced_timestamp
from
	{{ socotra_db }}.policy_modification as pm
	left join {{ socotra_db }}.peril_characteristics as pc on pm.locator = pc.policy_modification_locator
	join {{ socotra_db }}.policy_characteristics as poc on poc.locator = pc.policy_characteristics_locator
	join {{ socotra_db }}.policy_characteristics_fields as pcf on pcf.policy_characteristics_locator = poc.locator
	where parent_name = 'ad_insured'
group by pm.locator, pm.policy_locator,  poc.locator, poc.start_timestamp, poc.end_timestamp, poc.datamart_created_timestamp, poc.datamart_updated_timestamp, poc.replaced_timestamp
{% endmacro %}
