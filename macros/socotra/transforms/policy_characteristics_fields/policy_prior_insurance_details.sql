{% macro run_socotra_policy_prior_insurance(socotra_db, sf_schema) %}

select
	min(case when field_name = 'prior_carrier_name' then field_value end) as prior_carrier_name,
	min(case when field_name = 'prior_insurance' then field_value end) as prior_insurance,
	min(case when field_name = 'prior_policy_expiration_date' then field_value end) as prior_policy_expiration_date,
	policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
    	on pc.locator = pcf.policy_characteristics_locator
	where parent_name = 'prior_insurance_details'
{% if is_incremental() %}
    and (to_timestamp_tz(pcf.datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.policy_prior_insurance_details order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_prior_insurance_details order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
