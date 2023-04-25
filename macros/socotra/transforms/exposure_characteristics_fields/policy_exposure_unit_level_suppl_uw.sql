{% macro run_socotra_exposure_unit_level_suppl_uw(socotra_db, sf_schema) %}

select
	max(case when field_name = 'burglar_alarm' then field_value end) as burglar_alarm,
	max(case when field_name = 'business_description' then field_value end) as business_description,
	max(case when field_name = 'business_employees_on_premises' then field_value::boolean end) as business_employees_on_premises,
	max(case when field_name = 'business_on_premises' then field_value::boolean end) as business_on_premises,
	max(case when field_name = 'daycare_on_premises' then field_value::boolean end) as daycare_on_premises,
	max(case when field_name = 'diving_board' then field_value::boolean end) as diving_board,
	max(case when field_name = 'four_feet_fence' then field_value::boolean end) as four_feet_fence,
	max(case when field_name = 'mortgage' then field_value::boolean end) as mortgage,
	max(case when field_name = 'please_describe' then field_value end) as please_describe,
	max(case when field_name = 'property_with_fire_protection' then field_value::boolean end) as property_with_fire_protection,
	max(case when field_name = 'roof_payment_schedule' then field_value::boolean end) as roof_payment_schedule,
	max(case when field_name = 'secure_rails' then field_value::boolean end) as secure_rails,
	max(case when field_name = 'source_of_heat' then field_value::boolean end) as source_of_heat,
	max(case when field_name = 'source_of_heat_installation' then field_value::boolean end) as source_of_heat_installation,
	max(case when field_name = 'swimming_pool' then field_value::boolean end) as swimming_pool,
	max(case when field_name = 'thermo_static_control' then field_value::boolean end) as thermo_static_control,
	max(case when field_name = 'trampoline_liability' then field_value::boolean end) as trampoline_liability,
	max(case when field_name = 'trampoline_safety_net' then field_value::boolean end) as trampoline_safety_net,
	max(case when field_name = 'type_of_fuel' then field_value end) as type_of_fuel,
	max(case when field_name = 'unit_is_tied' then field_value::boolean end) as unit_is_tied,
	max(case when field_name = 'unrepaired_damages' then field_value::boolean end) as unrepaired_damages,
	max(case when field_name = 'utility_services' then field_value::boolean end) as utility_services,
	max(case when field_name = 'visitors_in_a_month' then field_value end) as visitors_in_a_month,
	max(case when field_name = 'wrought_iron' then field_value::boolean end) as wrought_iron,
	exposure_locator,
	ecf.exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	policy_modification_locator,
	to_timestamp_tz(ec.start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(ec.end_timestamp/1000) as end_timestamp,
	to_timestamp_tz(ec.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ec.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	inner join {{ socotra_db }}.peril_characteristics as pc
		on ec.locator = pc.exposure_characteristics_locator
	where parent_name = 'unit_level_suppl_uw'
{% if is_incremental() %}
    and (to_timestamp_tz(ec.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_unit_level_suppl_uw order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ec.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_unit_level_suppl_uw order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, policy_modification_locator, ec.start_timestamp, ec.end_timestamp
{% endmacro %}
