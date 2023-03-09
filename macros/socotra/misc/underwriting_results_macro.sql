{% macro run_socotra_policy_underwriting_results(socotra_db, sf_schema) %}

select
    locator,
    policy_locator,
    quote_locator,
    endorsement_locator,
    renewal_locator,
    decision,
    to_timestamp_tz(decision_timestamp/1000) as decision_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    policy_modification_locator
from {{ socotra_db }}.underwriting_results
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.underwriting_results order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.underwriting_results order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}