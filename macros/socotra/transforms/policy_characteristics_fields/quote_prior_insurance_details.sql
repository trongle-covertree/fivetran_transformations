{% macro run_socotra_quote_prior_insurance(socotra_db, sf_schema) %}

select
	max(case when field_name = 'prior_carrier_name' then field_value end) as prior_carrier_name,
	max(case when field_name = 'prior_insurance' then field_value end) as prior_insurance,
	max(case when field_name = 'prior_policy_expiration_date' then field_value end) as prior_policy_expiration_date,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'prior_insurance_details'
{% if is_incremental() %}
    and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_prior_insurance_details order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_prior_insurance_details order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
