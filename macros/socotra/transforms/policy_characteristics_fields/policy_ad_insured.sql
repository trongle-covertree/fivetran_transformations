{% macro run_socotra_ad_insured(socotra_db, sf_schema) %}

select
	max(case when field_name = 'additionalinsured_date_of_birth' then field_value::date end) as additionalinsured_date_of_birth,
	max(case when field_name = 'ad_first_name' then field_value end) as ad_first_name,
	max(case when field_name = 'ad_last_name' then field_value end) as ad_last_name,
	max(case when field_name = 'relationship_to_policyholder' then field_value end) as relationship_to_policyholder,
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
	where parent_name = 'ad_insured'
{% if is_incremental() %}
    and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_ad_insured order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_ad_insured order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator
{% endmacro %}
