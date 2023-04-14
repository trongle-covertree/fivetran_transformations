{% macro run_socotra_quote_peril_general_chars(socotra_db, sf_schema) %}

select
	max(Case when field_name = 'animal_liability_limit' then regexp_replace(field_value, '[$,]') end) animal_liability_limit,
	max(Case when field_name = 'cov_a_settlement_option' then field_value end) cov_a_settlement_option,
	max(Case when field_name = 'manufactured_home_limit' then field_value::int End) manufactured_home_limit,
	max(Case when field_name = 'unscheduled_personal_property_limit' then field_value::int end) unscheduled_personal_property_limit,
	max(Case when field_name = 'cov_c_settlement_option' then field_value End) cov_c_settlement_option,
	max(Case when field_name = 'personal_liability' then field_value End) personal_liability,
	max(Case when field_name = 'loss_of_use_percentage' then regexp_replace(field_value, '[%]')/100 end) loss_of_use_percentage,
	max(Case when field_name = 'medical_payment_to_others_limit_PerPerson' then field_value end) medical_payment_to_others_limit_PerPerson,
	max(Case when field_name = 'damage_to_property_of_others' then regexp_replace(field_value, '[$,]')::int end) damage_to_property_of_others,
	max(Case when field_name = 'all_other_perils_deductible' then regexp_replace(field_value, '[$,]')::int end) all_other_perils_deductible,
	max(Case when field_name = 'wind_hail_deductible' then regexp_replace(field_value, '[$,]')::int End) wind_hail_deductible,
	max(Case when field_name = 'fungi_bacteria_property_limit' then regexp_replace(field_value, '[$,]')::int End) fungi_bacteria_property_limit,
	max(Case when field_name = 'water_damage_reduced_limit' then field_value end) water_damage_reduced_limit,
	max(Case when field_name = 'specific_building_structure_description' then field_value End) specific_building_structure_description,
	max(Case when field_name = 'identity_fraud_limit' then regexp_replace(field_value, '[$,]')::int end) identity_fraud_limit,
	max(Case when field_name = 'scheduled_personals' then field_value End) scheduled_personals,
	max(Case when field_name = 'earthquake_deductible' then regexp_replace(field_value, '[%]')/100 end) earthquake_deductible,
	max(Case when field_name = 'inflation_guard' then regexp_replace(field_value, '[%]')/100 end) inflation_guard,
	max(Case when field_name = 'other_structures_limit' then field_value::int end) other_structures_limit,
	max(Case when field_name = 'cov_b_settlement_option' then field_value end) cov_b_settlement_option,
	max(Case when field_name = 'premises_liability_limit' then regexp_replace(field_value, '[$,]')::int End) premises_liability_limit,
	max(Case when field_name = 'no_of_golf_carts' then field_value::smallint End) no_of_golf_carts,
	max(Case when field_name = 'debris_removal_limit' then regexp_replace(field_value, '[$,]')::int end) debris_removal_limit,
	max(Case when field_name = 'loss_assessment_limit' then regexp_replace(field_value, '[$,]')::int End) loss_assessment_limit,
	max(Case when field_name = 'water_backup_and_sump_overflow_limit' then regexp_replace(field_value, '[$,]')::int end) water_backup_and_sump_overflow_limit,
	max(Case when field_name = 'medical_payment_to_others_limit_per_person' then field_value End) medical_payment_to_others_limit_per_person,
	max(Case when field_name = 'occasional_vacation_rental' then field_value::boolean end) occasional_vacation_rental,
	max(Case when field_name = 'residence_burglary_limit' then regexp_replace(field_value, '[$,]')::int End) residence_burglary_limit,
	max(Case when field_name = 'equipment_breakdown_limit' then regexp_replace(field_value, '[$,]')::int end) equipment_breakdown_limit,
	quote_exposure_locator,
	pc.quote_policy_locator,
	pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_peril_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_peril_characteristics as pc
		on pc.locator = pcf.quote_peril_characteristics_locator
	inner join {{ socotra_db }}.quote_peril as p
		on p.locator = pc.quote_peril_locator
where parent_name is null
{% if is_incremental() %}
    and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_peril_general_characteristics order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_peril_general_characteristics order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by quote_exposure_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
