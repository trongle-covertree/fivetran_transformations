{% macro run_socotra_quote_peril_sched_personals(socotra_db, sf_schema) %}

select
	max(Case when field_name = 'spp_value' then regexp_replace(field_value, '[$,]')::int end) spp_value,
	max(Case when field_name = 'spp_type' then field_value end) spp_type,
	max(Case when field_name = 'spp_desc' then field_value End) spp_desc,
	quote_exposure_locator,
	pc.quote_policy_locator,
	pc.policy_locator::varchar as policy_locator,
	parent_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_peril_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_peril_characteristics as pc
		on pc.locator = pcf.quote_peril_characteristics_locator
	inner join {{ socotra_db }}.quote_peril as p
		on p.locator = pc.quote_peril_locator
where parent_name = 'scheduled_personals'
group by quote_exposure_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator, parent_locator
{% endmacro %}
