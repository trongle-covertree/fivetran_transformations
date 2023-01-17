{{ config(materialized='table') }}

select sk AS claim_locator, field_values:documents as documents, to_varchar(field_values:injured_party_info[0]) as injured_party_info,
    to_varchar(field_values:loss_address[0]) as loss_address, to_varchar(field_values:loss_type[0]) as loss_type, to_varchar(field_values:policy_type[0]) as policy_type,
    to_varchar(field_values:property_city[0]) as property_city, to_varchar(field_values:property_country[0]) as property_country,
    to_varchar(field_values:property_county[0]) as property_county, to_varchar(field_values:property_zip_code[0]) as property_zip_code,
    to_varchar(field_values:relationship_to_insured[0]) as relationship_to_insured, to_varchar(field_values:reporting_party_first_name[0]) as reporting_party_first_name,
    to_varchar(field_values:reporting_party_last_name[0]) as reporting_party_last_name, to_varchar(field_values:reporting_party_phone_number[0]) as reporting_party_phone_number,
    to_varchar(field_values:theft_vandalism_claim_type[0]) as theft_vandalism_claim_type, to_varchar(field_values:theft_vandalism_description[0]) as theft_vandalism_description,
    to_varchar(field_values:source_of_damage_weather[0]) as source_of_damage_weather, to_varchar(field_values:weather_damage_repaired[0]) as weather_damage_repaired,
    to_varchar(field_values:weather_property_damage_description[0]) as weather_property_damage_description, field_values:emergency_service_contact as emergency_service_contact,
    to_varchar(field_values:fire_damage_description[0]) as fire_damage_description, to_varchar(field_values:fire_department[0]) as fire_department,
    to_varchar(field_values:source_of_damage_water_damage[0]) as source_of_damage_water_damage, to_varchar(field_values:water_damage_loss_description[0]) as water_damage_loss_description,
    to_varchar(field_values:water_damage_repaired[0]) as water_damage_repaired, to_varchar(field_values:liability_claim_reason[0]) as liability_claim_reason,
    to_varchar(field_values:liability_loss_description[0]) as liability_loss_description, to_varchar(field_values:other_loss_description[0]) as other_loss_description    
from fivetran_covertree.dynamodb.prod_socotra_claims_table where pk like 'POLICY#%'