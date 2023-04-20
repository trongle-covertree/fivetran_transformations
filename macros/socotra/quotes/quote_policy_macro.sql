{% macro run_socotra_quote_policy(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    policyholder_locator::varchar as policyholder_locator,
    policy_locator::varchar as policy_locator,
    name,
    product_name,
    payment_schedule_name,
    state,
    selected,
	to_timestamp_tz(contract_start_timestamp/1000) as contract_start_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_ntz(contract_start_timestamp/1000))) as contract_start_date,
    to_timestamp_tz(contract_end_timestamp/1000) as contract_end_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_ntz(contract_end_timestamp/1000))) as contract_end_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('Etc/GMT', 'America/New_York', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_et,
    currency,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.quote_policy order by created_timestamp desc limit 1) {# the {{this}} might need to be more explicit with {{ sf_schema }}.policy #}
    or to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_policy order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_policy order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}