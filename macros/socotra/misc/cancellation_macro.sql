{% macro run_socotra_cancellation(socotra_db, sf_schema, dynamo_schema) %}

select
    to_varchar(sc.locator) as locator,
    to_varchar(sc.reinstatement_locator) as reinstatement_locator,
    sc.name,
    dc.cancellation_category,
    to_varchar(sc.policy_locator) as policy_locator,
    sc.state,
    to_timestamp_tz(sc.effective_timestamp/1000) as effective_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(sc.effective_timestamp/1000))) as effective_date,
    iff(effective_date = to_date(convert_timezone('America/Los_Angeles', current_timestamp())), true, false) effective_is_current_date,
    to_timestamp_tz(sc.created_timestamp/1000) as created_timestamp,
    to_timestamp_tz(sc.datamart_created_timestamp/1000) as datamart_created_timestamp,
    to_timestamp_tz(sc.datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    to_varchar(sc.policy_modification_locator) as policy_modification_locator,
    to_timestamp_tz(sc.issued_timestamp/1000) as issued_timestamp,
    to_date(convert_timezone('America/Los_Angeles', to_timestamp_ntz(sc.issued_timestamp/1000))) as issued_date_pt,
    to_date(convert_timezone('America/New_York', to_timestamp_ntz(sc.issued_timestamp/1000))) as issued_date_et
from {{ socotra_db }}.cancellation as sc
    left join {{ dynamo_schema }}.prod_policies_cancellation as dc on dc.locator = sc.locator
{% if is_incremental() %}
where ( to_timestamp_tz(sc.created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.cancellation order by created_timestamp desc limit 1)
    or to_timestamp_tz(sc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.cancellation order by datamart_created_timestamp desc limit 1)
    or to_timestamp_tz(sc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.cancellation order by datamart_updated_timestamp desc limit 1))
    and _fivetran_deleted = false
{% endif %}

{% endmacro %}