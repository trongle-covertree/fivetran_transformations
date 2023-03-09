{% macro run_socotra_policyholder_fields(socotra_db, sf_schema) %}

select
	field_name,
    field_value,
    policyholder_locator::varchar as policyholder_locator,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    id,
    version
from {{ socotra_db }}.policyholder_fields
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policyholder_fields order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policyholder_fields order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}