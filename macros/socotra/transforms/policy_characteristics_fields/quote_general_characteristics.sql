{% macro run_socotra_quote_general_characteristics(socotra_db, sf_schema) %}

Select
    max(case when field_name = 'paperless_discount' then field_value::boolean end) as paperless_discount,
    max(case when field_name = 'association_discount' then field_value::boolean end) as association_discount,
    max(case when field_name = 'ncf_reference_name' then field_value end) as ncf_reference_name,
    max(case when field_name = 'ncf_status' then field_value end) as ncf_status,
    max(case when field_name = 'multi_policy_discount' then field_value::boolean end) as multi_policy_discount,
    max(case when field_name = 'policy_level_suppl_uw' then field_value end) as policy_level_suppl_uw,
    max(case when field_name = 'prior_insurance_details' then field_value end) as prior_insurance_details,
    max(case when field_name = 'application_intiation' then field_value end) as application_intiation,
    max(case when field_name = 'ncf_processing_status' then field_value end) as ncf_processing_status,
    max(case when field_name = 'insurance_score' then field_value::int end) as insurance_score,
    max(case when field_name = 'agency_information' then field_value end) as agency_information,
    max(case when field_name = 'previous_address_policyholder' then field_value end) as previous_address_policyholder,
    max(case when field_name = 'policy_basic_info' then field_value end) as policy_basic_info,
    max(case when field_name = 'documents' then field_value end) as documents,
    max(case when field_name = 'uw_details' then field_value end) as uw_details,
    max(case when field_name = 'adverse_action' then field_value end) as adverse_action,
    max(case when field_name = 'ad_insured' then field_value end) as ad_insured,
    max(case when field_name = 'prior_claims' then field_value end) as prior_claims,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
    where parent_name is null
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
