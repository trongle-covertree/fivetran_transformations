{% macro run_socotra_peril_sched_personals(socotra_db, sf_schema) %}

select
	max(Case when field_name = 'spp_value' then regexp_replace(field_value, '[$,]')::int end) spp_value,
	max(Case when field_name = 'spp_type' then field_value end) spp_type,
	max(Case when field_name = 'spp_desc' then field_value End) spp_desc,
	exposure_locator,
	pc.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	parent_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.start_timestamp/1000)) as start_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(pc.end_timestamp/1000)) as end_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.peril_characteristics_fields as pcf
	inner join {{ socotra_db }}.peril_characteristics as pc
		on pc.locator = pcf.peril_characteristics_locator
	inner join {{ socotra_db }}.peril as p
		on p.locator = pc.peril_locator
where parent_name = 'scheduled_personals'
group by exposure_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator, parent_locator, pc.start_timestamp, pc.end_timestamp
{% endmacro %}
