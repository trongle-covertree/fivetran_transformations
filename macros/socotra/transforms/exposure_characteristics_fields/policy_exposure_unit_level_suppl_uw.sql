{% macro run_socotra_exposure_unit_level_suppl_uw(socotra_db, sf_schema) %}

select
	min(case when field_name = 'unrepaired_damages' then field_value::boolean end) as unrepaired_damages,
	min(case when field_name = 'utility_services' then field_value::boolean end) as utility_services,
	min(case when field_name = 'business_on_premises' then field_value::boolean end) as business_on_premises,
	min(case when field_name = 'trampoline_liability' then field_value::boolean end) as trampoline_liability,
	min(case when field_name = 'mortgage' then field_value::boolean end) as mortgage,
	min(case when field_name = 'property_with_fire_protection' then field_value::boolean end) as property_with_fire_protection,
	min(case when field_name = 'secure_rails' then field_value::boolean end) as secure_rails,
	min(case when field_name = 'thermo_static_control' then field_value::boolean end) as thermo_static_control,
	min(case when field_name = 'swimming_pool' then field_value::boolean end) as swimming_pool,
	min(case when field_name = 'unit_is_tied' then field_value::boolean end) as unit_is_tied,
	min(case when field_name = 'source_of_heat' then field_value::boolean end) as source_of_heat,
	min(case when field_name = 'visitors_in_a_month' then field_value end) as visitors_in_a_month,
	min(case when field_name = 'business_employees_on_premises' then field_value::boolean end) as business_employees_on_premises,
	min(case when field_name = 'daycare_on_premises' then field_value::boolean end) as daycare_on_premises,
	min(case when field_name = 'burglar_alarm' then field_value end) as burglar_alarm,
	min(case when field_name = 'wrought_iron' then field_value::boolean end) as wrought_iron,
	min(case when field_name = 'trampoline_safety_net' then field_value::boolean end) as trampoline_safety_net,
	min(case when field_name = 'diving_board' then field_value::boolean end) as diving_board,
	min(case when field_name = 'four_feet_fence' then field_value::boolean end) as four_feet_fence,
	min(case when field_name = 'source_of_heat_installation' then field_value::boolean end) as source_of_heat_installation,
	min(case when field_name = 'type_of_fuel' then field_value end) as type_of_fuel,
	exposure_locator,
	exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ecf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ecf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	where parent_name = 'unit_level_suppl_uw'
{% if is_incremental() %}
    and (to_timestamp_tz(ecf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_unit_level_suppl_uw order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ecf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_unit_level_suppl_uw order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ecf.datamart_created_timestamp, ecf.datamart_updated_timestamp, ec.policy_locator
{% endmacro %}
