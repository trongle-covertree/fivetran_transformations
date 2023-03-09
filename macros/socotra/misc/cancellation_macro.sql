{% macro run_socotra_cancellation(socotra_db, sf_schema) %}

select
    to_varchar(locator) as locator,
    to_varchar(reinstatement_locator) as reinstatement_locator,
    name,
    to_varchar(policy_locator) as policy_locator,
    state,
    to_timestamp_tz(effective_timestamp/1000) as effective_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(effective_timestamp/1000))) as effective_date,
    iff(effective_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) effective_is_current_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    to_varchar(policy_modification_locator) as policy_modification_locator,
    to_timestamp_tz(issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('America/New_York', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_et,
    iff(issued_date_pt = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) as issued_is_current_date
from {{ socotra_db }}.cancellation
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.cancellation order by created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.cancellation order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.cancellation order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}