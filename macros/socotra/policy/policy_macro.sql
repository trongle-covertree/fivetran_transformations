{% macro run_socotra_policy(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
	to_timestamp_tz(policy_start_timestamp/1000) as policy_start_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(policy_start_timestamp/1000))) as policy_start_date,
    to_timestamp_tz(policy_end_timestamp/1000) as policy_end_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(policy_start_timestamp/1000))) as policy_end_date,
    to_timestamp_tz(issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('America/New_York', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_et,
    iff(issued_date_pt = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) issued_is_current_date,
    payment_schedule_name,
    policyholder_locator::varchar as policyholder_locator,
    product_name,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    premium_report_name,
    config_version,
    to_timestamp_tz(cancellation_timestamp/1000) as cancellation_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(cancellation_timestamp/1000))) as cancellation_date,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    currency
from {{ socotra_db }}.policy
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp) > (select created_timestamp from {{ sf_schema }}.policy order by created_timestamp desc limit 1) {# the {{this}} might need to be more explicit with {{ sf_schema }}.policy #}
    or to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.policy order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.policy order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}