{% macro run_socotra_peril_characteristics(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    peril_locator::varchar as peril_locator,
    policy_locator::varchar as policy_locator,
    replacement_of_locator::varchar as replacement_of_locator,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(start_timestamp/1000))) as start_date,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(end_timestamp/1000))) as end_date,
    to_timestamp_tz(replaced_timestamp/1000) as replaced_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(replaced_timestamp/1000))) as replaced_date_pt,
    to_date(convert_timezone('America/New_York', to_timestamp_ntz(replaced_timestamp/1000))) as replaced_date_et,
    to_timestamp_tz(issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('America/New_York', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_et,
    iff(issued_date_pt = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) issued_is_current_date,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    month_premium,
    premium,
    month_technical_premium,
    technical_premium,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    exposure_characteristics_locator::varchar as exposure_characteristics_locator,
    policy_characteristics_locator::varchar as policy_characteristics_locator,
    policy_modification_locator::varchar as policy_modification_locator
from {{ socotra_db }}.peril_characteristics
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp) > (select created_timestamp from {{ sf_schema }}.peril_characteristics order by created_timestamp desc limit 1) {# the {{this}} might need to be more explicit with {{ sf_schema }}.policy #}
    or to_timestamp_tz(datamart_created_timestamp) > (select datamart_created_timestamp from {{ sf_schema }}.peril_characteristics order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp) > (select datamart_updated_timestamp from {{ sf_schema }}.peril_characteristics order by datamart_updated_timestamp desc limit 1))
{% endif %}

{% endmacro %}