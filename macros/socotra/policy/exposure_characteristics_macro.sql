{% macro run_socotra_exposure_characteristics(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    policy_locator::varchar as policy_locator,
    exposure_locator::varchar as exposure_locator,
    replacement_of_locator::varchar as replacement_of_locator,
    to_timestamp_tz(start_timestamp/1000) as start_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_ntz(start_timestamp/1000))) as start_date,
    to_timestamp_tz(end_timestamp/1000) as end_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_ntz(end_timestamp/1000))) as end_date,
    to_timestamp_tz(replaced_timestamp/1000) as replaced_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_ntz(replaced_timestamp/1000))) as replaced_date_pt,
    to_date(convert_timezone('Etc/GMT', 'America/New_York', to_timestamp_ntz(replaced_timestamp/1000))) as replaced_date_et,
    to_timestamp_tz(issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('Etc/GMT', 'America/New_York', to_timestamp_ntz(issued_timestamp/1000))) as issued_date_et,
    to_timestamp_tz(created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.exposure_characteristics
{% if is_incremental() %}
where ( to_timestamp_tz(created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.exposure_characteristics order by created_timestamp desc limit 1) {# the {{this}} might need to be more explicit with {{ sf_schema }}.policy #}
    or to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.exposure_characteristics order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.exposure_characteristics order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}