{% macro run_socotra_quote_agency_information(socotra_db, sf_schema) %}

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
	quote_policy_characteristics_locator,
	quote_policy_locator,
     pc.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(pc.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from {{ socotra_db }}.quote_policy_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_policy_characteristics as pc
          on pc.locator = pcf.quote_policy_characteristics_locator
     where parent_name = 'agency_information'
group by pcf.quote_policy_characteristics_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
