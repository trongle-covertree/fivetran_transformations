{% macro run_socotra_policy_financial_transaction(socotra_db, sf_schema) %}

select
    policy_locator::varchar as policy_locator,
    id,
	invoice_locator,
    invoice_schedule_id,
    policy_modification_locator,
    peril_characteristics_locator,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    amount,
    type,
    peril_name,
    commission_recipient,
    fee_name,
    tax_name,
    to_timestamp_tz(posted_timestamp/1000) as posted_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(posted_timestamp/1000))) as posted_date,
    iff(posted_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) posted_is_current_date,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.financial_transaction
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.financial_transaction order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.financial_transaction order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}