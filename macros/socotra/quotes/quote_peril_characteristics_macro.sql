{% macro run_socotra_quote_peril_characteristics(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    quote_policy_locator::varchar as quote_policy_locator,
    policy_locator::varchar as policy_locator,
    quote_peril_locator::varchar as quote_peril_locator,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_tz(start_timestamp/1000))) as start_date,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_tz(end_timestamp/1000))) as end_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    month_premium,
    premium,
    month_technical_premium,
    technical_premium,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_peril_characteristics
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.quote_peril_characteristics order by created_timestamp desc limit 1) {# the {{this}} might need to be more explicit with {{ sf_schema }}.policy #}
    or to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_peril_characteristics order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_peril_characteristics order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}