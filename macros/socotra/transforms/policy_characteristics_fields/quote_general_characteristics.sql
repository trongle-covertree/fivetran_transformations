{% macro run_socotra_quote_general_characteristics(socotra_db, sf_schema) %}

Select
    min(case when field_name = 'paperless_discount' then field_value::boolean end) as paperless_discount,
    min(case when field_name = 'association_discount' then field_value::boolean end) as association_discount,
    min(case when field_name = 'ncf_reference_name' then field_value end) as ncf_reference_name,
    min(case when field_name = 'ncf_status' then field_value end) as ncf_status,
    min(case when field_name = 'multi_policy_discount' then field_value::boolean end) as multi_policy_discount,
    min(case when field_name = 'policy_level_suppl_uw' then field_value end) as policy_level_suppl_uw,
    min(case when field_name = 'prior_insurance_details' then field_value end) as prior_insurance_details,
    min(case when field_name = 'application_intiation' then field_value end) as application_intiation,
    min(case when field_name = 'ncf_processing_status' then field_value end) as ncf_processing_status,
    min(case when field_name = 'insurance_score' then field_value::int end) as insurance_score,
    min(case when field_name = 'agency_information' then field_value end) as agency_information,
    min(case when field_name = 'previous_address_policyholder' then field_value end) as previous_address_policyholder,
    min(case when field_name = 'policy_basic_info' then field_value end) as policy_basic_info,
    min(case when field_name = 'documents' then field_value end) as documents,
    min(case when field_name = 'uw_details' then field_value end) as uw_details,
    min(case when field_name = 'adverse_action' then field_value end) as adverse_action,
    min(case when field_name = 'ad_insured' then field_value end) as ad_insured,
    min(case when field_name = 'prior_claims' then field_value end) as prior_claims,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
    where parent_name is null
{% if is_incremental() %}
    and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_general_characteristics order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_general_characteristics order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.quote_policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
