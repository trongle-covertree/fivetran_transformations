{% macro run_socotra_grace_period(socotra_db, sf_schema) %}

select
    locator::varchar as locator,
    policy_locator::varchar as policy_locator,
    cancellation_locator,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(start_timestamp/1000))) as start_date,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(end_timestamp/1000))) as end_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(cancel_effective_timestamp/1000) as cancel_effective_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(cancel_effective_timestamp/1000))) as cancel_effective_date,
    iff(cancel_effective_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) cancel_eff_is_current_date,
    to_timestamp_tz(settled_timestamp/1000) as settled_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.grace_period
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp) > (select created_timestamp from {{ sf_schema }}.grace_period order by created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.grace_period order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.grace_period order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}