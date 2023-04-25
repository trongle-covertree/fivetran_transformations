{% macro run_socotra_policy_invoice(socotra_db, sf_schema) %}

select
    locator::varchar as locator,
    policy_locator::varchar as policy_locator,
    total_due,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_timestamp_tz(issue_timestamp/1000) as issued_timestamp,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
	settlement_status,
    settlement_type,
    to_timestamp_tz(due_timestamp/1000) as due_timestamp,
    grace_period_locator,
    status,
    to_timestamp_tz(updated_timestamp/1000) as updated_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    display_id
from {{ socotra_db }}.invoice
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.invoice order by created_timestamp desc limit 1)
    or to_timestamp_tz(updated_timestamp/1000) > (select updated_timestamp from {{ sf_schema }}.invoice order by updated_timestamp desc limit 1)
    or to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.invoice order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.invoice order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}