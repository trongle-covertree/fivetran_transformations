{% macro run_socotra_quote_peril_characteristics_fields(socotra_db, sf_schema) %}

select
	field_name,
    field_value,
    quote_peril_characteristics_locator::varchar quote_peril_characteristics_locator,
    parent_name,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    parent_locator::varchar as parent_locator,
    is_group,
    id
from {{ socotra_db }}.quote_peril_characteristics_fields
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.quote_peril_characteristics_fields order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_peril_characteristics_fields order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}