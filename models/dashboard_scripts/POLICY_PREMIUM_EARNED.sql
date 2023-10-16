{{ config(materialized='table') }}

SELECT
	cal.REPORT_DATE,
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
	NULL AS CANCELLATION_FLAG,
	RENEWAL_FLAG,
	0 AS TOTAL_PREMIUM_WITH_REPLACED_AMOUNT,
	0 AS GROSS_WRITTEN_PREMIUM_AMOUNT,
	0 AS GROSS_WRITTEN_PREMIUM_AMOUNT_CHANGE,
--	PERIL_END_DATE - PERIL_START_DATE AS PERIL_DURATION,
	1 AS DAILY_ACTIVE_FLAG,
	GROSS_WRITTEN_PREMIUM_AMOUNT/ (PERIL_END_DATE-PERIL_START_DATE) AS EARNED_PREMIUM_AMOUNT
FROM {{ ref('POLICY_PREMIUM_GROSS_WRITTEN_v2') }} AS p
inner JOIN
(
	SELECT
		REPORT_DATE
	FROM TRANSFORMATIONS_DYNAMODB_DEV_RAJA.DIM_CALENDAR
	WHERE REPORT_DATE <= CURRENT_DATE()) cal
on cal.REPORT_DATE>= p.PERIL_START_DATE
AND cal.REPORT_DATE< p.PERIL_END_DATE
AND cal.REPORT_DATE<=CURRENT_DATE()
WHERE REPLACEMENT_FLAG =0
AND GROSS_WRITTEN_PREMIUM_AMOUNT<>0