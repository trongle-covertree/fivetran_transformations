{% macro run_socotra_policy_underwriting_notes(socotra_db, sf_schema) %}

select
    underwriting_result_locator,
    note,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.underwriting_notes
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.underwriting_notes order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.underwriting_notes order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}