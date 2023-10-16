{{ config(materialized='table') }}

SELECT 
	REPORT_DATE,
	ISSUED_DATE,
	POLICY_START_DATE,
	POLICY_END_DATE,
	PERIL_START_DATE,
	PERIL_END_DATE,
	REPLACED_DATE,
	CANCELLATION_DATE,
	CANCELLATION_REASON,
	RENEWAL_DATE,
	POLICYHOLDER_LOCATOR,
	POLICY_LOCATOR,
	EXPOSURE_LOCATOR,
	EXPOSURE_CHARACTERISTICS_LOCATOR,
	PERIL_CHARACTERISTIC_LOCATOR,
	REPLACEMENT_OF_PERIL_CHARACTERISTIC_LOCATOR ,
	PERIL_LOCATOR,
	PAYMENT_SCHEDULE_NAME,
	PRODUCT_NAME, 
	PERIL_NAME,
	EXPOSURE_NAME,
	REPLACEMENT_FLAG,
	CANCELLATION_FLAG,
	RENEWAL_FLAG,
	DAILY_ACTIVE_FLAG,
	SUM(GROSS_WRITTEN_PREMIUM_AMOUNT) AS GROSS_WRITTEN_PREMIUM_AMOUNT,
	SUM(EARNED_PREMIUM_AMOUNT) AS EARNED_PREMIUM_AMOUNT,
	sum(GROSS_WRITTEN_PREMIUM_AMOUNT_CHANGE) AS GROSS_WRITTEN_PREMIUM_AMOUNT_CHANGE
FROM 
(
SELECT
	REPORT_DATE,
	ISSUED_DATE,
	POLICY_START_DATE,
	POLICY_END_DATE,
	PERIL_START_DATE,
	PERIL_END_DATE,
	REPLACED_DATE,
	CANCELLATION_DATE,
	CANCELLATION_REASON,
	RENEWAL_DATE,
	POLICYHOLDER_LOCATOR,
	POLICY_LOCATOR,
	EXPOSURE_LOCATOR,
	EXPOSURE_CHARACTERISTICS_LOCATOR,
	PERIL_CHARACTERISTIC_LOCATOR,
	REPLACEMENT_OF_PERIL_CHARACTERISTIC_LOCATOR ,
	PERIL_LOCATOR,
	PAYMENT_SCHEDULE_NAME,
	PRODUCT_NAME, 
	PERIL_NAME,
	EXPOSURE_NAME,
	REPLACEMENT_FLAG,
	CANCELLATION_FLAG,
	RENEWAL_FLAG,
	CASE WHEN GROSS_WRITTEN_PREMIUM_AMOUNT >0 THEN 1 ELSE 0 END DAILY_ACTIVE_FLAG,
	GROSS_WRITTEN_PREMIUM_AMOUNT,
	GROSS_WRITTEN_PREMIUM_AMOUNT_CHANGE,
	EARNED_PREMIUM_AMOUNT
FROM {{ ref('POLICY_PREMIUM_GROSS_WRITTEN_v2') }}
--WHERE GROSS_WRITTEN_PREMIUM_AMOUNT<>0
UNION ALL
SELECT
	REPORT_DATE,
	ISSUED_DATE,
	POLICY_START_DATE,
	POLICY_END_DATE,
	PERIL_START_DATE,
	PERIL_END_DATE,
	REPLACED_DATE,
	CANCELLATION_DATE,
	CANCELLATION_REASON,
	RENEWAL_DATE,
	POLICYHOLDER_LOCATOR,
	POLICY_LOCATOR,
	EXPOSURE_LOCATOR,
	EXPOSURE_CHARACTERISTICS_LOCATOR,
	PERIL_CHARACTERISTIC_LOCATOR,
	REPLACEMENT_OF_PERIL_CHARACTERISTIC_LOCATOR ,
	PERIL_LOCATOR,
	PAYMENT_SCHEDULE_NAME,
	PRODUCT_NAME, 
	PERIL_NAME,
	EXPOSURE_NAME,
	REPLACEMENT_FLAG,
	CANCELLATION_FLAG,
	RENEWAL_FLAG,
	DAILY_ACTIVE_FLAG,
	GROSS_WRITTEN_PREMIUM_AMOUNT,
	GROSS_WRITTEN_PREMIUM_AMOUNT_CHANGE,
	EARNED_PREMIUM_AMOUNT
FROM {{ ref('POLICY_PREMIUM_EARNED') }} D
) p
GROUP BY 
	REPORT_DATE,
	ISSUED_DATE,
	POLICY_START_DATE,
	POLICY_END_DATE,
	PERIL_START_DATE,
	PERIL_END_DATE,
	REPLACED_DATE,
	CANCELLATION_REASON,
	CANCELLATION_DATE,
	RENEWAL_DATE,
	POLICYHOLDER_LOCATOR,
	POLICY_LOCATOR,
	EXPOSURE_LOCATOR,
	EXPOSURE_CHARACTERISTICS_LOCATOR,
	PERIL_CHARACTERISTIC_LOCATOR,
	REPLACEMENT_OF_PERIL_CHARACTERISTIC_LOCATOR ,
	PERIL_LOCATOR,
	PAYMENT_SCHEDULE_NAME,
	PRODUCT_NAME, 
	PERIL_NAME,
	EXPOSURE_NAME,
	REPLACEMENT_FLAG,
	CANCELLATION_FLAG,
	RENEWAL_FLAG,
	DAILY_ACTIVE_FLAG
ORDER BY 
	ISSUED_DATE,
	PERIL_START_DATE,
	PERIL_END_DATE,
	REPORT_DATE