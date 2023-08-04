{% macro run_socotra_policy_agency_information(socotra_db, sf_schema) %}

select
     max(case when field_name = 'country' then field_value end) as country,
     max(case when field_name = 'county' then field_value end) as county,
     max(case when field_name = 'city' then field_value end) as city,
     max(case when field_name = 'agent_id' then field_value::varchar end) as agent_id,
     max(case when field_name = 'lot_unit' then field_value end) as lot_unit,
     max(case when field_name = 'agency_id' then field_value::varchar end) as agency_id,
     max(case when field_name = 'agency_contact_name' then field_value end) as agency_contact_name,
     max(case when field_name = 'street_address' then field_value end) as street_address,
     max(case when field_name = 'zip_code' then field_value end) as zip_code,
     max(case when field_name = 'agency_phone_number' then field_value end) as agency_phone_number,
     max(case when field_name = 'email_address' then field_value end) as email_address,
     max(case when field_name = 'state' then field_value end) as state,
     max(case when field_name = 'agency_license' then field_value::varchar end) as agency_license,
     max(case when field_name = 'agent_on_record' then field_value end) as agent_on_record,
	pcf.policy_characteristics_locator,
     pc.policy_locator::varchar as policy_locator,
     policy_modification_locator,
	to_timestamp_tz(pc.start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(pc.end_timestamp/1000) as end_timestamp,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp,
    to_timestamp_tz(pc.replaced_timestamp/1000) as replaced_timestamp
from {{ socotra_db }}.policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.policy_characteristics as pc
          on pc.locator = pcf.policy_characteristics_locator
    inner join {{ socotra_db }}.peril_characteristics as p
        on p.policy_characteristics_locator = pc.locator
     where parent_name = 'agency_information'
{% if is_incremental() %}
     and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_agency_information order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_agency_information order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.policy_locator, policy_modification_locator, pc.start_timestamp, pc.end_timestamp, pc.replaced_timestamp
{% endmacro %}
