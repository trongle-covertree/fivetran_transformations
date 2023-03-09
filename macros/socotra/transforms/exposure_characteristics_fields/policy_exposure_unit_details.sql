{% macro run_socotra_exposure_unit_details(socotra_db, sf_schema) %}

select
	min(case when field_name = 'acv' then regexp_replace(field_value, '[,]')::double end) as acv,
	min(case when field_name = 'community_policy_discount' then field_value end) as community_policy_discount,
	min(case when field_name = 'form' then field_value end) as form,
	min(case when field_name = 'rcv' then regexp_replace(field_value, '[,]')::double end) as rcv,
	min(case when field_name = 'valuation_id' then field_value end) as valuation_id,
	min(case when field_name = 'unit_id' then field_value::varchar end) as unit_id,
	min(case when field_name = 'unit_location' then field_value end) as unit_location,
	min(case when field_name = 'purchase_date' then field_value end) as purchase_date,
	min(case when field_name = 'personalized_plan_type' then field_value end) as personalized_plan_type,
	min(case when field_name = 'park_name' then field_value end) as park_name,
	min(case when field_name = 'unusual_risk' then field_value end) as unusual_risk,
	min(case when field_name = 'rcv_360value' then regexp_replace(field_value, '[,]')::double end) as rcv_360value,
	exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ecf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ecf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	where parent_name = 'unit_details'
{% if is_incremental() %}
    and (to_timestamp_tz(ecf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_unit_details order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ecf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_unit_details order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by ecf.exposure_characteristics_locator, ecf.datamart_created_timestamp, ecf.datamart_updated_timestamp, ec.policy_locator
{% endmacro %}
