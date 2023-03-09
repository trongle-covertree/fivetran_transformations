{% macro run_socotra_renewal(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    policy_locator::varchar as policy_locator,
    state,
    new_payment_schedule,
    to_timestamp_tz(issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('America/New_York', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_et,
    iff(issued_date_pt = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) issued_is_current_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    policy_modification_locator::varchar as policy_modification_locator
from {{ socotra_db }}.renewal
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.renewal order by created_timestamp desc limit 1) {# the {{this}} might need to be more explicit with {{ sf_schema }}.policy #}
    or to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.renewal order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.renewal order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}