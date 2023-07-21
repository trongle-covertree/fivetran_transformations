{% macro run_socotra_policy_modification(socotra_db, sf_schema) %}

select
	locator::varchar as locator,
    policy_locator::varchar as policy_locator,
    name,
    to_timestamp_tz(effective_timestamp/1000) as effective_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_tz(effective_timestamp/1000))) as effective_date,
    type,
    to_timestamp_tz(datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    to_timestamp_tz(issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('Etc/GMT', 'America/Los_Angeles', to_timestamp_tz(issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('Etc/GMT', 'America/New_York', to_timestamp_tz(issued_timestamp/1000))) as issued_date_et,
    premium_change,
    gross_taxes_change,
    fee_change
from {{ socotra_db }}.policy_modification
{% if is_incremental() %}
where (
    to_timestamp_tz(datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_modification order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_modification order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}