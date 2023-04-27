{% macro run_socotra_claim_general(socotra_db, sf_schema) %}

select
	max(case when field_name = 'documents' then field_value end) as documents,
	max(case when field_name = 'emergency_service_contact' then field_value::varchar end) as emergency_service_contact,
	max(case when field_name = 'fire_damage_description' then field_value end) as fire_damage_description,
	max(case when field_name = 'fire_department' then field_value end) as fire_department,
	max(case when field_name = 'injured_party_info' then field_value end) as injured_party_info,
	max(case when field_name = 'liability_claim_reason' then field_value end) as liability_claim_reason,
	max(case when field_name = 'liability_loss_description' then field_value end) as liability_loss_description,
	max(case when field_name = 'loss_address' then field_value end) as loss_address,
	max(case when field_name = 'loss_type' then field_value end) as loss_type,
	max(case when field_name = 'other_loss_description' then field_value end) as other_loss_description,
	max(case when field_name = 'policy_type' then field_value end) as policy_type,
	max(case when field_name = 'property_street_address' then field_value end) as property_street_address,
	max(case when field_name = 'property_lot_unit#' then field_value end) as property_lot_unit,
	max(case when field_name = 'property_city' then field_value end) as property_city,
	max(case when field_name = 'property_state' then field_value end) as property_state,
	max(case when field_name = 'property_zip_code' then field_value::varchar end) as property_zip_code,
	max(case when field_name = 'property_county' then field_value end) as property_county,
	max(case when field_name = 'property_country' then field_value end) as property_country,
	max(case when field_name = 'relationship_to_insured' then field_value end) as relationship_to_insured,
	max(case when field_name = 'reporting_party_email_address' then field_value end) as reporting_party_email_address,
	max(case when field_name = 'reporting_party_first_name' then field_value end) as reporting_party_first_name,
	max(case when field_name = 'reporting_party_last_name' then field_value end) as reporting_party_last_name,
	max(case when field_name = 'reporting_party_phone_number' then field_value end) as reporting_party_phone_number,
	max(case when field_name = 'source_of_damage_water_damage' then field_value end) as source_of_damage_water_damage,
	max(case when field_name = 'source_of_damage_weather' then field_value end) as source_of_damage_weather,
	max(case when field_name = 'theft_vandalism_claim_type' then field_value end) as theft_vandalism_claim_type,
	max(case when field_name = 'theft_vandalism_description' then field_value end) as theft_vandalism_description,
	max(case when field_name = 'water_damage_loss_description' then field_value end) as water_damage_loss_description,
	max(case when field_name = 'water_damage_repaired' then field_value end) as water_damage_repaired,
	max(case when field_name = 'weather_damage_repaired' then field_value end) as weather_damage_repaired,
	max(case when field_name = 'weather_property_damage_description' then field_value end) as weather_property_damage_description,
	claim_locator,
    c.policy_locator::varchar as policy_locator,
	discarded,
	to_timestamp_tz(c.created_timestamp/1000) as created_timestamp,
	to_timestamp_tz(c.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(c.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.claim_fields as cf
	inner join {{ socotra_db }}.claim as c
		on cf.claim_locator = c.locator
	where parent_name is null
{% if is_incremental() %}
    and (to_timestamp_tz(c.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_claim_general order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(c.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_claim_general order by datamart_updated_timestamp desc limit 1)
	  or to_timestamp_tz(c.created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.policy_claim_general order by created_timestamp desc limit 1))
{% endif %}
group by claim_locator, c.policy_locator, discarded, c.created_timestamp, c.datamart_created_timestamp, c.datamart_updated_timestamp
{% endmacro %}
