{% macro run_socotra_policy_characteristics_fields(socotra_db, sf_schema) %}

select
	field_name,
    field_value,
    policy_characteristics_locator::varchar as policy_characteristics_locator,
    parent_name,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    parent_locator::varchar as parent_locator,
    is_group,
    id
from {{ socotra_db }}.policy_characteristics_fields
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.policy_characteristics_fields order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_characteristics_fields order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}