{% macro run_socotra_policy_invoice_schedule(socotra_db, sf_schema) %}

select
    id,
    product_name,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(start_timestamp/1000))) as start_date,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(end_timestamp/1000))) as end_date,
    to_timestamp_tz(issue_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(issue_timestamp/1000))) as issued_date,
    iff(issued_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) issued_is_current_date,
    to_timestamp_tz(due_timestamp/1000) as due_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(due_timestamp/1000))) as due_date,
    iff(due_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) due_date_is_current_date,
	write_off,
    policy_locator::varchar as policy_locator,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    deleted,
    invoice_locator::varchar as invoice_locator
from {{ socotra_db }}.invoice_schedule
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.invoice_schedule order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.invoice_schedule order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}