{% macro run_socotra_policy_auxiliary_data(socotra_db, sf_schema) %}

select
    locator,
    key_name,
    var_value,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.auxiliary_data
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.auxiliary_data order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.auxiliary_data order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}
