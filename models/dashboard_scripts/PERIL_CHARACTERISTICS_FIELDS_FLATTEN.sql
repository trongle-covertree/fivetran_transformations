{{ config(materialized='table') }}

SELECT 
	PERIL_CHARACTERISTICS_LOCATOR, 
	POLICY_LOCATOR,
	EXPOSURE_LOCATOR,
	EXPOSURE_CHARACTERISTICS_LOCATOR,
	PERIL_NAME,
	CASE WHEN ALL_OTHER_PERILS_DEDUCTIBLE IS NULL THEN 'No' else ALL_OTHER_PERILS_DEDUCTIBLE END                                    AS ALL_OTHER_PERILS_DEDUCTIBLE,
	CASE WHEN ANIMAL_LIABILITY_LIMIT IS NULL THEN 'No' else ANIMAL_LIABILITY_LIMIT END                                              AS ANIMAL_LIABILITY_LIMIT,
	CASE WHEN COV_A_SETTLEMENT_OPTION IS NULL THEN 'No' else COV_A_SETTLEMENT_OPTION END                                            AS COV_A_SETTLEMENT_OPTION,
	CASE WHEN COV_B_SETTLEMENT_OPTION IS NULL THEN 'No' else COV_B_SETTLEMENT_OPTION END                                            AS COV_B_SETTLEMENT_OPTION,
	CASE WHEN COV_C_SETTLEMENT_OPTION IS NULL THEN 'No' else COV_C_SETTLEMENT_OPTION END                                            AS COV_C_SETTLEMENT_OPTION,
	CASE WHEN DAMAGE_TO_PROPERTY_OF_OTHERS IS NULL THEN 'No' else DAMAGE_TO_PROPERTY_OF_OTHERS END                                  AS DAMAGE_TO_PROPERTY_OF_OTHERS,
	CASE WHEN DEBRIS_REMOVAL_LIMIT IS NULL THEN 250 else DEBRIS_REMOVAL_LIMIT END                                                   AS DEBRIS_REMOVAL_LIMIT,
	CASE WHEN EARTHQUAKE_DEDUCTIBLE IS NULL THEN 'No' else EARTHQUAKE_DEDUCTIBLE END                                                AS EARTHQUAKE_DEDUCTIBLE,
	CASE WHEN EQUIPMENT_BREAKDOWN_LIMIT IS NULL THEN 'No' else EQUIPMENT_BREAKDOWN_LIMIT END                                        AS EQUIPMENT_BREAKDOWN_LIMIT,
	CASE WHEN FUNGI_BACTERIA_PROPERTY_LIMIT IS NULL THEN 'No' else FUNGI_BACTERIA_PROPERTY_LIMIT END                                AS FUNGI_BACTERIA_PROPERTY_LIMIT,
	CASE WHEN IDENTITY_FRAUD_LIMIT IS NULL THEN 'No' else IDENTITY_FRAUD_LIMIT END                                                  AS IDENTITY_FRAUD_LIMIT,
	CASE WHEN INFLATION_GUARD IS NULL THEN 'No' else INFLATION_GUARD END                                                            AS INFLATION_GUARD,
	CASE WHEN LOSS_ASSESSMENT_LIMIT IS NULL THEN 'No' else LOSS_ASSESSMENT_LIMIT END                                                AS LOSS_ASSESSMENT_LIMIT,
	CASE WHEN LOSS_OF_USE_PERCENTAGE IS NULL THEN '0.0' else LOSS_OF_USE_PERCENTAGE END                                             AS LOSS_OF_USE_PERCENTAGE,
	CASE WHEN MANUFACTURED_HOME_LIMIT IS NULL THEN 'No' else MANUFACTURED_HOME_LIMIT END                                            AS MANUFACTURED_HOME_LIMIT,
	CASE WHEN MEDICAL_PAYMENT_TO_OTHERS_LIMIT_PERPERSON IS NULL THEN 'No' else MEDICAL_PAYMENT_TO_OTHERS_LIMIT_PERPERSON END        AS MEDICAL_PAYMENT_TO_OTHERS_LIMIT_PERPERSON,
	CASE WHEN MEDICAL_PAYMENT_TO_OTHERS_LIMIT_PER_PERSON IS NULL THEN 'No' else MEDICAL_PAYMENT_TO_OTHERS_LIMIT_PER_PERSON END      AS MEDICAL_PAYMENT_TO_OTHERS_LIMIT_PER_PERSON,
	CASE WHEN NO_OF_GOLF_CARTS IS NULL THEN 0 else NO_OF_GOLF_CARTS END                                                             AS NO_OF_GOLF_CARTS,
	CASE WHEN OCCASIONAL_VACATION_RENTAL IS NULL THEN 'No' else OCCASIONAL_VACATION_RENTAL END                                      AS OCCASIONAL_VACATION_RENTAL,
	CASE WHEN OTHER_STRUCTURES_LIMIT IS NULL THEN 'No' else OTHER_STRUCTURES_LIMIT END                                              AS OTHER_STRUCTURES_LIMIT,
	CASE WHEN PERSONAL_LIABILITY IS NULL THEN 'No' else PERSONAL_LIABILITY END                                                      AS PERSONAL_LIABILITY,
	CASE WHEN PREMISES_LIABILITY_LIMIT IS NULL THEN 'No' else PREMISES_LIABILITY_LIMIT END                                          AS PREMISES_LIABILITY_LIMIT,
	CASE WHEN RESIDENCE_BURGLARY_LIMIT IS NULL THEN 'No' else RESIDENCE_BURGLARY_LIMIT END                                          AS RESIDENCE_BURGLARY_LIMIT,
	CASE WHEN SCHEDULED_PERSONALS IS NULL THEN 'No' else SCHEDULED_PERSONALS END                                                    AS SCHEDULED_PERSONALS,
	CASE WHEN SECONDARY_RESIDENCE_LIABILITY_ADDRESS IS NULL THEN 'No' else SECONDARY_RESIDENCE_LIABILITY_ADDRESS END                AS SECONDARY_RESIDENCE_LIABILITY_ADDRESS,
	CASE WHEN SECONDARY_RESIDENCE_LIABILITY_GROUP IS NULL THEN 'No' else SECONDARY_RESIDENCE_LIABILITY_GROUP END                    AS SECONDARY_RESIDENCE_LIABILITY_GROUP,
	CASE WHEN SPECIFIC_BUILDING_STRUCTURE_DESCRIPTION IS NULL THEN 'No' else SPECIFIC_BUILDING_STRUCTURE_DESCRIPTION END            AS SPECIFIC_BUILDING_STRUCTURE_DESCRIPTION,
	CASE WHEN SPP_DESC IS NULL THEN 'No' else SPP_DESC END                                                                          AS SPP_DESC,
	CASE WHEN SPP_TYPE IS NULL THEN 'No' else SPP_TYPE END                                                                          AS SPP_TYPE,
	CASE WHEN SPP_VALUE IS NULL THEN 'No' else SPP_VALUE END                                                                        AS SPP_VALUE,
	CASE WHEN UNSCHEDULED_PERSONAL_PROPERTY_LIMIT IS NULL THEN 'No' else UNSCHEDULED_PERSONAL_PROPERTY_LIMIT END                    AS UNSCHEDULED_PERSONAL_PROPERTY_LIMIT,
	CASE WHEN WATER_BACKUP_AND_SUMP_OVERFLOW_LIMIT IS NULL THEN 'No' else WATER_BACKUP_AND_SUMP_OVERFLOW_LIMIT END                  AS WATER_BACKUP_AND_SUMP_OVERFLOW_LIMIT,
	CASE WHEN WATER_DAMAGE_REDUCED_LIMIT IS NULL THEN 'No' else WATER_DAMAGE_REDUCED_LIMIT END                                      AS WATER_DAMAGE_REDUCED_LIMIT,
	CASE WHEN WIND_HAIL_DEDUCTIBLE IS NULL THEN 'No' else WIND_HAIL_DEDUCTIBLE END                                                  AS WIND_HAIL_DEDUCTIBLE
FROM {{ ref('PERIL_CHARACTERISTICS_FIELDS_FLATTEN_STAGE1') }}