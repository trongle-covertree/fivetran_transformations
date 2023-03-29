{% macro run_socotra_quote_prior_claims(socotra_db, sf_schema) %}

select
	min(case when field_name = 'claim_amount' then field_value::double end) as claim_amount,
	min(case when field_name = 'prior_claims_subject_last_name' then field_value end) as prior_claims_subject_last_name,
	min(case when field_name = 'prior_claims_subject_first_name' then field_value end) as prior_claims_subject_first_name,
	min(case when field_name = 'claim_source' then field_value end) as claim_source,
	min(case when field_name = 'category' then field_value end) as category,
	min(case when field_name = 'claim_date' then field_value::date end) as claim_date,
	min(case when field_name = 'claim_status' then field_value end) as claim_status,
	min(case when field_name = 'claim_number' then field_value end) as claim_number,
	min(case when field_name = 'claim_cat' then field_value::boolean end) as claim_cat,
	min(case when field_name = 'reference_number' then field_value end) as reference_number,
	min(case when field_name = 'scope_of_claim' then field_value end) as scope_of_claim,
	min(case when field_name = 'claims_processing_status' then field_value end) as claims_processing_status,
	min(case when field_name = 'description_of_loss' then field_value end) as description_of_loss,
	quote_policy_characteristics_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'prior_claims'
{% if is_incremental() %}
    and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_policy_prior_claims order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_policy_prior_claims order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.quote_policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
