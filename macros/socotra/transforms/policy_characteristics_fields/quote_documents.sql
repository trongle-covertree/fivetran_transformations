{% macro run_socotra_quote_documents(socotra_db, sf_schema) %}

select
	max(case when field_name = 'uploaded_file' then field_value end) as uploaded_file,
	max(case when field_name = 'document_type' then field_value end) as document_type,
	quote_policy_characteristics_locator,
	quote_policy_locator,
    pc.policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
    	on pc.locator = pcf.quote_policy_characteristics_locator
	where parent_name = 'documents'
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
