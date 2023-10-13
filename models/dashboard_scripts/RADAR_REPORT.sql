{{ config(materialized='table') }}

SELECT
	ec.STATE,
	a.POLICY_LOCATOR || '_' || COALESCE(ec.UNIT_ID,'') 			as "Risk ID",
	Date(convert_timezone('America/New_York', to_timestamp_tz(p.POLICY_START_TIMESTAMP/1000))) as "Policy_Term_Effective_Date",
	Date(convert_timezone('America/New_York', to_timestamp_tz(p.POLICY_START_TIMESTAMP/1000))) as "Original_Effective_Date",
	ec.LAT || ', ' || ec.LONG                                   as "Lat_Long",
	ec.GRID_ID                                                  as "GridID",
	'NA'                                                        as "Census Tract",
	ec.COUNTY                                                   as "County",
	ec.ZIP_CODE                                                 as "Zip",
	ec.CITY                                                     as "PO Name", --check
	'NA'                                                        as "Place Name",
	'NA'                                                        as "County Tier",
	ec.WINDPOOL                                                 as "Windpool",
	'NA'                                                        as "HUD Zone",
	'NA'                                                        as "Zillow_Community",
	'NA'                                                        as "UA_Label",
	ec.DISTANCE_TO_COAST                                        as "DtC",
	'NA'                                                        as "Gate Type",
	ec.POLICY_USAGE                                             as "Occupancy",
	ecs.MANUFACTURED_HOME_LIMIT                                 as "Cov A Limit",
	ecs.OTHER_STRUCTURES_LIMIT                                  as "Cov B Limit",
	ecs.UNSCHEDULED_PERSONAL_PROPERTY_LIMIT                     as "Cov C Limit",
	ecs.LOSS_OF_USE_PERCENTAGE                                  as "Cov D %",  -- CHANGE TO decimal
	ec.MODEL_YEAR                                               as "Year Built",
	pcff.DATE_OF_BIRTH                                          as "Date of Birth",
	pcff.INSURANCE_SCORE                                        as "Insurance Score",
	ec.UNIT_LOCATION                                            as "Community Status",
	ecs.PERSONAL_LIABILITY                                      as "Personal_Liability",
	ecs.PREMISES_LIABILITY_LIMIT                                as "Premises_Liability",
	ec.ROOF_YEAR_YYYY                                           as "Roof Year",
	ec.ROOF_MATERIAL                                            as "Roof Material",
	ecs.COV_A_SETTLEMENT_OPTION                                 as "DW Settlement",
	ecs.COV_C_SETTLEMENT_OPTION                                 as "PP Settlement",
	ecs.WIND_HAIL_EXCLUSION                                     as "WH Exclusion", --check
	ecs.ALL_OTHER_PERILS_DEDUCTIBLE                             as "Ded - AOP",
	ecs.WIND_HAIL_DEDUCTIBLE                                    as "Ded - Wind/Hail",
	'NA'                                                        as "Claims",
	CASE
		WHEN p.PAYMENT_SCHEDULE_NAME IN ('11-Pay','2-Pay') THEN 'Payment Plan'
		WHEN PAYMENT_SCHEDULE_NAME ='Mortgagee Bill' THEN 'Lienholder/ Mortgage Billed'
		ELSE p.PAYMENT_SCHEDULE_NAME 
	END        						                            as "Paid In full",
	'NA'                                                        as "Hurricane Target - All",
	'NA'                                                        as "Hurricane Target - A",
	'NA'                                                        as "Hurricane Target - B",
	'NA'                                                        as "Hurricane Target - C",
	ec.HOME_TYPE                                                as "Home Type",
	ec.FORM                                                     as "Form",
	case
        when pcff.application_intiation in ('Internal Agent','Direct to Consumer') then 'Direct to Consumer'
        when pcff.application_intiation = 'Independent Agent' then 'Brokered'
        ELSE pcff.application_intiation
    end                                   						as "Channel", -- TO BE UPDATED
	ec.ROOF_CONDITION                                           as "Roof-- Condition",
	pcff.ASSOCIATION_DISCOUNT                                   as "Affinity/Association",
	pcff.AUTO_POLICY_WITH_AGENCY                                as "Auto/Home",
	ec.COMMUNITY_POLICY_DISCOUNT                                as "Community Policy",
	1                                                           as "Multi-Policy",
	UNIT_COUNT                                                  as "Multi-Unit",
	ec.STORM_MITIGATION_FORTIFIED                               as "D - Fortified",
	ec.STORM_MITIGATION_IMPACTGLASS                             as "D - Impact Glass",
	ec.STORM_MITIGATION_SHUTTERS                                as "D - Shutters",
	CASE
		WHEN pcff.PRIOR_INSURANCE ='Yes, I am thinking about switching carriers' THEN 0
		WHEN pcff.PRIOR_INSURANCE = 'No, I don''t have insurance today' AND PCFF.PRIOR_POLICY_EXPIRATION_DATE ='My home has never been insured' THEN 91
		WHEN PCFF.PRIOR_INSURANCE IN ('No, I''m buying a new home','No, I don''t have insurance today') THEN 
			CASE 
				WHEN PCFF.PRIOR_POLICY_EXPIRATION_DATE = '1-7 days ago' THEN 0
				WHEN PCFF.PRIOR_POLICY_EXPIRATION_DATE  = '8-30 days ago' THEN 8
				WHEN PCFF.PRIOR_POLICY_EXPIRATION_DATE = '31-90 days ago' THEN 31
				WHEN PCFF.PRIOR_POLICY_EXPIRATION_DATE = 'More than 90 days ago' THEN 91
				ELSE 0
			END
		ELSE 0
	END                                                        as "Prior Lapse Days",
	ec.SHORT_TERM_RENTAL_SURCHARGE                             as "Short-Term Rental",
	pcff.PAPERLESS_DISCOUNT                                    as "Paperless",
	ec.SOURCE_OF_HEAT_INSTALLATION                             as "Supplemental Heating Source",
	ecs.EARTHQUAKE_DEDUCTIBLE                                  as "Earthquake",
	a.POLICY_TERM                                              as "Policy_Term", 
	ec.UNUSUAL_RISK                                            as "Unusual_Risk",
	ecs.ENHANCED_COVERAGE                                      as "Enhanced",
	ecs.NO_OF_GOLF_CARTS                                       as "Golf_Cart",
	ecs.HOBBY_FARMING                                          as "Hobby_Farming",
	ecs.IDENTITY_FRAUD_LIMIT                                   as "Identity_Fraud",
	ecs.SPECIFIC_BUILDING_EXCLUSION                            as "Specific_Building_Exclusion",
	500                                                        as "Fire_Dept_Service_Charge",
	ecs.VANDALISM_MALICIOUS_MISCHIEF                           as "Vandalism_Malicious_Mischief",
	ecs.BUILDERS_RISK                                          as "Builders_Risk",
	ecs.THEFT_LIMITATION                                       as "Theft_Limitation",
	ec.TRAMPOLINE_LIABILITY                                    as "Trampoline_Liability_Ext",
	ec.DIVING_BOARD                                            as "Diving_Board_Slide_Ext",
	ecs.ANIMAL_LIABILITY_LIMIT                                 as "Animal_Liability",
	ecs.DEBRIS_REMOVAL_LIMIT                                   as "Debris_Removal",
	ecs.EQUIPMENT_BREAKDOWN_LIMIT                              as "Equipment_Breakdown",
	ecs.FUNGI_BACTERIA_PROPERTY_LIMIT                          as "Fungi_Mold_Property",
	ec.AERIAL                                                  as "Inspection_Aerial",
	ec.EXTERIOR                                                as "Inspection_Exterior",
	ec.INTERIOR                                                as "Inspection_Interior",
	ecs.LANDLORD_PERSONAL_INJURY                               as "Landlord_Personal_Injury",
	'No'                                                       as "Limited_Bed_Bug",
	ecs.LOSS_ASSESSMENT                                        as "Loss_Assessment",
	ecs.MEDICAL_PAYMENT_TO_OTHERS_LIMIT_PERPERSON              as "Medical_Payments",
	OCCASIONAL_VACATION_RENTAL                                 as "Occasional_Rental",
	'No'                                                       as "Pet_Damage",
	ecs.RESIDENCE_BURGLARY_LIMIT                               as "Residence_Burglary",
	ecs.SPP_TYPE1                                              as "SPP_Type1",
	ecs.SPP_LIMIT1                                             as "SPP_Limit1",
	ecs.SPP_TYPE2                                              as "SPP_Type2",
	ecs.SPP_LIMIT2                                             as "SPP_Limit2",
	ecs.SPP_TYPE3                                              as "SPP_Type3",
	ecs.SPP_LIMIT3                                             as "SPP_Limit3",
	ecs.SPP_TYPE4                                              as "SPP_Type4",
	ecs.SPP_LIMIT4                                             as "SPP_Limit4",
	ecs.SPP_TYPE5                                              as "SPP_Type5",
	ecs.SPP_LIMIT5                                             as "SPP_Limit5",
	ecs.SPP_TYPE6                                              as "SPP_Type6",
	ecs.SPP_LIMIT6                                             as "SPP_Limit6",
	ecs.SPP_TYPE7                                              as "SPP_Type7",
	ecs.SPP_LIMIT7                                             as "SPP_Limit7",
	ecs.SPP_TYPE8                                              as "SPP_Type8",
	ecs.SPP_LIMIT8                                             as "SPP_Limit8",
	ecs.SPP_TYPE9                                              as "SPP_Type9",
	ecs.SPP_LIMIT9                                             as "SPP_Limit9",
	ecs.SPP_TYPE10                                             as "SPP_Type10",
	ecs.SPP_LIMIT10                                            as "SPP_Limit10",
	ecs.SECONDARY_RESIDENCE_COUNT                              as "Secondary_Residence_Count",
	ecs.TRIP_COLLISION                                         as "Trip_Collision",
	ecs.WATER_BACKUP_AND_SUMP_OVERFLOW_LIMIT                   as "Water_Backup",
	ecs.WATER_DAMAGE_REDUCED_LIMIT                             as "Water_Damage_Limit",
	ecs.ROOF_EXCLUSION                                         as "Roof_Exclusion",
	'No'                                                       as "HailSettlementBuyBack",
	ec.BURGLAR_ALARM                                           as "Burglar_Alarm",
	ec.WROUGHT_IRON                                            as "Wrought_Iron_Bars",
	ECS.MINE_SUBSIDENCE_OHIO                                   as "Mine_Subsidence_Ohio",
	ecs.MINE_SUBSIDENCE_INDIANA_DWELLING                       as "Mine_Subsidence_Indiana_Dwelling",
	ecs.MINE_SUBSIDENCE_INDIANA_OTHER_STRUCTURES               as "Mine_Subsidence_Indiana_Other_Structures",
	ecs.MINE_SUBSIDENCE_INDIANA_ADDITIONAL_LIVING_EXPENSE      as "Mine_Subsidence_Indiana_Additional_Living_Expense",
	ecs.MINE_SUBSIDENCE_ILLINOIS_DWELLING                      as "Mine_Subsidence_Illinois_Dwelling",
	ecs.MINE_SUBSIDENCE_ILLINOIS_OTHER_STRUCTURES              as "Mine_Subsidence_Illinois_Other_Structures",
	ecs.FORTIFIED_ROOF_UPGRADE                                 as "FORTIFED_Roof_Upgrade"
FROM {{ ref('DIM_POLICY_DURATION') }} a
LEFT JOIN MYSQL_DATA_MART_10001."POLICY" p
ON a.POLICY_LOCATOR =p."LOCATOR" 
LEFT JOIN {{ ref('POLICY_CHARACTERISTICS_FIELDS_FLATTEN') }} pcff
ON pcff.POLICY_LOCATOR =p."LOCATOR"
inner JOIN {{ ref('EXPOSURE_CHARACTERISTICS_OTHERS_SNAPSHOT') }} ecs
ON p."LOCATOR" =ecs.POLICY_LOCATOR 
LEFT JOIN {{ ref('EXPOSURE_CHARACTERISTICS_FIELDS_FLATTEN') }} ec
ON ec.EXPOSURE_CHARACTERISTICS_LOCATOR =ecs.EXPOSURE_CHARACTERISTICS_LOCATOR
LEFT JOIN 
	(SELECT 
		aa.POLICY_LOCATOR,
		count(DISTINCT bb.UNIT_ID) as UNIT_COUNT
	FROM {{ ref('EXPOSURE_CHARACTERISTICS_OTHERS_SNAPSHOT') }} aa
	LEFT JOIN {{ ref('EXPOSURE_CHARACTERISTICS_FIELDS_FLATTEN') }} bb
	ON aa.EXPOSURE_CHARACTERISTICS_LOCATOR=bb.EXPOSURE_CHARACTERISTICS_LOCATOR
	GROUP BY 1) x
ON a.POLICY_LOCATOR = x.POLICY_LOCATOR
WHERE CURRENT_DATE() BETWEEN POLICY_START_DATE AND POLICY_END_DATE 
AND CANCELLATION_DATE IS NULL