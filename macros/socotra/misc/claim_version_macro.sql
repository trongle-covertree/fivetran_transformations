{% macro run_socotra_claim_version(socotra_db, sf_schema) %}

select
    id,
	claim_locator::varchar as claim_locator,
    claim_status,
    to_timestamp_tz(notification_timestamp/1000) as notification_timestamp,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(incident_timestamp/1000) as incident_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.claim_version
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.claim_version order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.claim_version order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}