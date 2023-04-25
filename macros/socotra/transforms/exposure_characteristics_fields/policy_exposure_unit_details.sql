{% macro run_socotra_exposure_unit_details(socotra_db, sf_schema) %}

select
	max(case when field_name = 'acv' then regexp_replace(field_value, '[,]')::double end) as acv,
	max(case when field_name = 'community_policy_discount' then field_value end) as community_policy_discount,
	max(case when field_name = 'form' then field_value end) as form,
	max(case when field_name = 'rcv' then regexp_replace(field_value, '[,]')::double end) as rcv,
	max(case when field_name = 'valuation_id' then field_value end) as valuation_id,
	max(case when field_name = 'unit_id' then field_value::varchar end) as unit_id,
	max(case when field_name = 'unit_location' then field_value end) as unit_location,
	max(case when field_name = 'purchase_date' then field_value end) as purchase_date,
	max(case when field_name = 'personalized_plan_type' then field_value end) as personalized_plan_type,
	max(case when field_name = 'park_name' then field_value end) as park_name,
	max(case when field_name = 'unusual_risk' then field_value end) as unusual_risk,
	max(case when field_name = 'rcv_360value' then regexp_replace(field_value, '[,]')::double end) as rcv_360value,
	exposure_locator,
	ecf.exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ec.start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(ec.end_timestamp/1000) as end_timestamp,
	to_timestamp_tz(ec.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ec.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	where parent_name = 'unit_details'
{% if is_incremental() %}
    and (to_timestamp_tz(ec.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_unit_details order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ec.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_unit_details order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, ec.start_timestamp, ec.end_timestamp
{% endmacro %}
