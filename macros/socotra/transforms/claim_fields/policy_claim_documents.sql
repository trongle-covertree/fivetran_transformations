{% macro run_socotra_claim_documents(socotra_db, sf_schema) %}

select
	max(case when field_name = 'upload_file' then field_value end) as upload_file,
	claim_locator,
    c.policy_locator::varchar as policy_locator,
	discarded,
	to_timestamp_tz(c.created_timestamp/1000) as created_timestamp,
	to_timestamp_tz(c.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(c.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.claim_fields as cf
	inner join {{ socotra_db }}.claim as c
		on cf.claim_locator = c.locator
	where parent_name = 'documents'
{% if is_incremental() %}
    and (to_timestamp_tz(c.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_claim_documents order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(c.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_claim_documents order by datamart_updated_timestamp desc limit 1)
	  or to_timestamp_tz(c.created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.policy_claim_documents order by created_timestamp desc limit 1))
{% endif %}
group by claim_locator, c.policy_locator, discarded, c.created_timestamp, c.datamart_created_timestamp, c.datamart_updated_timestamp
{% endmacro %}
