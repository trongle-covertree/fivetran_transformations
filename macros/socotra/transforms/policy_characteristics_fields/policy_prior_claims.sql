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
    pm.locator as policy_modification_locator,
    pm.policy_locator,
    poc.locator as policy_characteristics_locator,
    convert_timezone('America/New_York', to_timestamp_tz(poc.start_timestamp/1000)) as start_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.end_timestamp/1000)) as end_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp,
    convert_timezone('America/New_York', to_timestamp_tz(poc.replaced_timestamp/1000)) as replaced_timestamp
from
	{{ socotra_db }}.policy_modification as pm
	left join {{ socotra_db }}.peril_characteristics as pc on pm.locator = pc.policy_modification_locator
	join {{ socotra_db }}.policy_characteristics as poc on poc.locator = pc.policy_characteristics_locator
	join {{ socotra_db }}.policy_characteristics_fields as pcf on pcf.policy_characteristics_locator = poc.locator
	where parent_name = 'prior_claims'
group by pm.locator, pm.policy_locator,  poc.locator, poc.start_timestamp, poc.end_timestamp, poc.datamart_created_timestamp, poc.datamart_updated_timestamp, poc.replaced_timestamp
{% endmacro %}
