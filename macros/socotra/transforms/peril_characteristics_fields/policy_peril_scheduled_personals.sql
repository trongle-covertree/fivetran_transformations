{% macro run_socotra_peril_sched_personals(socotra_db, sf_schema) %}

select
	min(Case when field_name = 'spp_value' then regexp_replace(field_value, '[$,]')::int end) spp_value,
	min(Case when field_name = 'spp_type' then field_value end) spp_type,
	min(Case when field_name = 'spp_desc' then field_value End) spp_desc,
	exposure_locator,
	pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.peril_characteristics_fields as pcf
	inner join {{ socotra_db }}.peril_characteristics as pc
		on pc.locator = pcf.peril_characteristics_locator
	inner join {{ socotra_db }}.peril as p
		on p.locator = pc.peril_locator
where parent_name = 'scheduled_personals'
{% if is_incremental() %}
    and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_peril_scheduled_personals order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_peril_scheduled_personals order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
