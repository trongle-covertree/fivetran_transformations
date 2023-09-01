{% macro run_socotra_quote_exposure_unit_details(socotra_db, sf_schema) %}

select
	max(case when field_name = 'acv' then regexp_replace(field_value, '[$,]') end) as acv,
	max(case when field_name = 'community_policy_discount' then field_value end) as community_policy_discount,
	max(case when field_name = 'form' then field_value end) as form,
	max(case when field_name = 'rcv' then regexp_replace(field_value, '[$,]') end) as rcv,
	max(case when field_name = 'valuation_id' then field_value end) as valuation_id,
	max(case when field_name = 'unit_id' then field_value::varchar end) as unit_id,
	max(case when field_name = 'unit_location' then field_value end) as unit_location,
	max(case when field_name = 'purchase_date' then field_value end) as purchase_date,
	max(case when field_name = 'personalized_plan_type' then field_value end) as personalized_plan_type,
	max(case when field_name = 'park_name' then field_value end) as park_name,
	max(case when field_name = 'unusual_risk' then field_value end) as unusual_risk,
	max(case when field_name = 'rcv_360value' then regexp_replace(field_value, '[$,]') end) as rcv_360value,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'unit_details'
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
