{% macro run_socotra_quote_agency_information(socotra_db, sf_schema) %}

select
     min(case when field_name = 'country' then field_value end) as country,
     min(case when field_name = 'county' then field_value end) as county,
     min(case when field_name = 'city' then field_value end) as city,
     min(case when field_name = 'agent_id' then field_value::varchar end) as agent_id,
     min(case when field_name = 'lot_unit' then field_value end) as lot_unit,
     min(case when field_name = 'agency_id' then field_value::varchar end) as agency_id,
     min(case when field_name = 'agency_contact_name' then field_value end) as agency_contact_name,
     min(case when field_name = 'street_address' then field_value end) as street_address,
     min(case when field_name = 'zip_code' then field_value end) as zip_code,
     min(case when field_name = 'agency_phone_number' then field_value end) as agency_phone_number,
     min(case when field_name = 'email_address' then field_value end) as email_address,
     min(case when field_name = 'state' then field_value end) as state,
     min(case when field_name = 'agency_license' then field_value::varchar end) as agency_license,
     min(case when field_name = 'agent_on_record' then field_value end) as agent_on_record,
	quote_policy_characteristics_locator,
     pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pcf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pcf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
          on pc.locator = pcf.quote_policy_characteristics_locator
     where parent_name = 'agency_information'
{% if is_incremental() %}
     and (to_timestamp_tz(pcf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_policy_agency_information order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pcf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_policy_agency_information order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by pcf.quote_policy_characteristics_locator, pcf.datamart_created_timestamp, pcf.datamart_updated_timestamp, pc.policy_locator
{% endmacro %}
