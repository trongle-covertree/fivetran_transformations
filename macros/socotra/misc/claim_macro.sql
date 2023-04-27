{% macro run_socotra_claim(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    policy_locator::varchar as policy_locator,
    product_name,
    discarded,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.claim
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.claim order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.claim order by datamart_updated_timestamp desc limit 1)
    or to_timestamp_tz(created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.claim order by created_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}