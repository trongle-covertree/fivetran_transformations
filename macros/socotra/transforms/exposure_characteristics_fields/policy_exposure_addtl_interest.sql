{% macro run_socotra_exposure_addtl_interest(socotra_db, sf_schema) %}

select
	min(case when field_name = 'account_number' then field_value end) as account_number,
	min(case when field_name = 'name' then field_value::varchar end) as name,
	min(case when field_name = 'additional_interest_id' then field_value end) as additional_interest_id,
	min(case when field_name = 'street_address' then field_value end) as street_address,
	min(case when field_name = 'lot_unit' then field_value end) as lot_unit,
	min(case when field_name = 'city' then field_value end) as city,
	min(case when field_name = 'state' then field_value end) as state,
	min(case when field_name = 'zip_code' then field_value end) as zip_code,
	min(case when field_name = 'county' then field_value end) as county,
	min(case when field_name = 'country' then field_value end) as country,
	min(case when field_name = 'type' then field_value end) as type,
	min(case when field_name = 'officer_first_name' then field_value end) as officer_first_name,
	min(case when field_name = 'officer_last_name' then field_value end) as officer_last_name,
	min(case when field_name = 'officer_mail_address' then field_value end) as officer_mail_address,
	exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ecf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ecf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	where parent_name = 'additional_interest'
{% if is_incremental() %}
    and (to_timestamp_tz(ecf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_addtl_interest order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ecf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_addtl_interest order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by ecf.exposure_characteristics_locator, ecf.datamart_created_timestamp, ecf.datamart_updated_timestamp, ec.policy_locator
{% endmacro %}
