{% macro run_socotra_quote_exposure_characteristics(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    quote_policy_locator::varchar as quote_policy_locator,
    policy_locator::varchar as policy_locator,
    quote_exposure_locator::varchar as quote_exposure_locator,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(start_timestamp/1000))) as start_date,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(end_timestamp/1000))) as end_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_exposure_characteristics
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp) > (select created_timestamp from {{ sf_schema }}.quote_exposure_characteristics order by created_timestamp desc limit 1) {# the {{this}} might need to be more explicit with {{ sf_schema }}.policy #}
    or to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.quote_exposure_characteristics order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_exposure_characteristics order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}