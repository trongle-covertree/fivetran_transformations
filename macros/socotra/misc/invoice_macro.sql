{% macro run_socotra_policy_invoice(socotra_db, sf_schema) %}

select
    locator::varchar as locator,
    policy_locator::varchar as policy_locator,
    total_due,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(start_timestamp/1000))) as start_date,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(end_timestamp/1000))) as end_date,
    to_timestamp_tz(issue_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(issue_timestamp/1000))) as issued_date,
    iff(issued_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) issued_is_current_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
	settlement_status,
    settlement_type,
    to_timestamp_tz(due_timestamp/1000) as due_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(due_timestamp/1000))) as due_date,
    iff(due_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) due_date_is_current_date,
    grace_period_locator,
    status,
    to_timestamp_tz(updated_timestamp/1000) as updated_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.invoice
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp) > (select created_timestamp from {{ sf_schema }}.invoice order by created_timestamp desc limit 1)
    or to_timestamp_tz(updated_timestamp) > (select updated_timestamp from {{ sf_schema }}.invoice order by updated_timestamp desc limit 1)
    or to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.invoice order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.invoice order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}