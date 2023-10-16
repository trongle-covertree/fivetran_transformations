{{ config(materialized='table') }}

SELECT 
--		REPORT_DATE,
	a.ISSUED_DATE AS REPORT_DATE, 
	PAYMENT_SCHEDULE_NAME,
--		EXPOSURE_NAME,
	1 AS ISSUED_POLICY_FLAG,
	CASE WHEN RENEWAL_FLAG=1 THEN 'Renew' ELSE 'New' END AS NEW_RENEWAL,
	'USA' AS COUNTRY,
	case when e.POLICY_STATE IS NULL THEN 'Unknown' ELSE e.POLICY_STATE END AS STATE,
	CASE WHEN b.PARK_NAME IS NULL THEN 'Unknown' ELSE b.PARK_NAME END AS PARK_NAME,
	'INDIVIDUAL' AS AGENCY_CHANNEL,
	c.AGENCY_ID AS AGENCY_ID,
	CASE WHEN c.AGENCY_NAME_UPDATED IS NOT NULL THEN c.AGENCY_NAME_UPDATED ELSE 'Unknown' END AS AGENCY_CONTACT_NAME, 
	c.AGENT_ID AS AGENT_ID,
	CASE WHEN c.AGENT_ON_RECORD IS NOT NULL THEN c.AGENT_ON_RECORD ELSE 'Unknown' END AS AGENT_ON_RECORD, 
	A.CANCELLATION_FLAG AS CANCELLATION_FLAG, 
	a.CANCELLATION_DATE -a.ISSUED_DATE AS CANCELLATION_DURATION,
	CASE 
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <0 THEN '-1'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  =0 THEN '1. Same Day'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <7 THEN '2. 1-7 days'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <30 THEN '3. 8-30 days'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <60 THEN '4. 31-60 days'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <90 THEN '5. 61-90 days'
		ELSE '6. 90+ days'
	END AS CANCELLATION_DURATION_BUCKET,
	CANCELLATION_REASON,
	a.POLICY_LOCATOR AS POLICY_COUNT_LOCATOR,
	NULL AS QUOTE_COUNT_LOCATOR,
	NULL AS CANCELLED_POLICY_COUNT_LOCATOR,
	SUM(GROSS_WRITTEN_PREMIUM_AMOUNT) AS GROSS_WRITTEN_PREMIUM_AMOUNT,
	sum(GROSS_WRITTEN_PREMIUM_AMOUNT_CHANGE) AS GROSS_WRITTEN_PREMIUM_AMOUNT_CHANGE,
	SUM(EARNED_PREMIUM_AMOUNT) AS EARNED_PREMIUM_AMOUNT
FROM {{ ref('POLICY_PREMIUM_AGG') }} a
LEFT JOIN {{ ref('EXPOSURE_CHARACTERISTICS_FIELDS_FLATTEN') }} b
ON a.EXPOSURE_CHARACTERISTICS_LOCATOR =b.EXPOSURE_CHARACTERISTICS_LOCATOR 
LEFT JOIN {{ ref('AGENCY_DETAILS') }} c
ON a.POLICY_LOCATOR =c.POLICY_LOCATOR  
AND a.ISSUED_DATE =c.POLICY_ISSUE_DATE 
--	AND (CASE WHEN RENEWAL_FLAG=1 THEN a.RENEWAL_DATE ELSE a.ISSUED_DATE END) =c.POLICY_ISSUE_DATE 
LEFT JOIN {{ ref('POLICY_CHARACTERISTICS') }} e
ON a.POLICY_LOCATOR =e.POLICY_LOCATOR
WHERE REPORT_DATE>='2022-01-01'
AND REPORT_DATE <= CURRENT_DATE()
GROUP BY 
	a.ISSUED_DATE, 
	PAYMENT_SCHEDULE_NAME,
	CASE WHEN RENEWAL_FLAG=1 THEN 'Renew' ELSE 'New' END,
	case when e.POLICY_STATE IS NULL THEN 'Unknown' ELSE e.POLICY_STATE END,
	CASE WHEN b.PARK_NAME IS NULL THEN 'Unknown' ELSE b.PARK_NAME END,
	c.AGENCY_ID,
	CASE WHEN c.AGENCY_NAME_UPDATED IS NOT NULL THEN c.AGENCY_NAME_UPDATED ELSE 'Unknown' END, 
	c.AGENT_ID,
	CASE WHEN c.AGENT_ON_RECORD IS NOT NULL THEN c.AGENT_ON_RECORD ELSE 'Unknown' END, 
	A.CANCELLATION_FLAG,
	a.POLICY_LOCATOR, 
	a.CANCELLATION_DATE -a.ISSUED_DATE,
	CASE 
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <0 THEN '-1'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  =0 THEN '1. Same Day'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <7 THEN '2. 1-7 days'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <30 THEN '3. 8-30 days'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <60 THEN '4. 31-60 days'
		WHEN a.CANCELLATION_DATE -a.ISSUED_DATE  <90 THEN '5. 61-90 days'
		ELSE '6. 90+ days'
	END,
	CANCELLATION_REASON