{% macro run_socotra_policy_prior_claims(socotra_db, sf_schema) %}

select
	max(case when field_name = 'claim_amount' then field_value::double end) as claim_amount,
	max(case when field_name = 'prior_claims_subject_last_name' then field_value end) as prior_claims_subject_last_name,
	max(case when field_name = 'prior_claims_subject_first_name' then field_value end) as prior_claims_subject_first_name,
	max(case when field_name = 'claim_source' then field_value end) as claim_source,
	max(case when field_name = 'category' then field_value end) as category,
	max(case when field_name = 'claim_date' then field_value::date end) as claim_date,
	max(case when field_name = 'claim_status' then field_value end) as claim_status,
	max(case when field_name = 'claim_number' then field_value end) as claim_number,
	max(case when field_name = 'claim_cat' then field_value::boolean end) as claim_cat,
	max(case when field_name = 'reference_number' then field_value end) as reference_number,
	max(case when field_name = 'scope_of_claim' then field_value end) as scope_of_claim,
	max(case when field_name = 'claims_processing_status' then field_value end) as claims_processing_status,
	max(case when field_name = 'description_of_loss' then field_value end) as description_of_loss,
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
	where parent_name = 'prior_claims'
{% if is_incremental() %}
    and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_prior_claims order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_prior_claims order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator
{% endmacro %}
