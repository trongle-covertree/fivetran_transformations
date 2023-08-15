{% macro run_socotra_policy_uw_details(socotra_db, sf_schema) %}

select
    max(case when field_name = 'uw_description' then field_value end) as uw_description,
    max(case when field_name = 'uw_manual' then field_value end) as uw_manual,
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
	where parent_name = 'uw_details'
{% if is_incremental() %}
	and (to_timestamp_tz(poc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_uw_details order by datamart_created_timestamp desc limit 1)
		or to_timestamp_tz(poc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_uw_details order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pm.locator, pm.policy_locator,  poc.locator, poc.start_timestamp, poc.end_timestamp, poc.datamart_created_timestamp, poc.datamart_updated_timestamp, poc.replaced_timestamp
{% endmacro %}
