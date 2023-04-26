{% macro run_socotra_claim_fields(socotra_db, sf_schema) %}

select
	claim_locator::varchar as claim_locator,
    field_name,
    field_value,
    parent_locator,
    is_group,
    parent_name,
    version,
    id,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.exposure_characteristics_fields
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.claim_fields order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.claim_fields order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}