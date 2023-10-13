{{ config(materialized='table') }}

SELECT 
	xx.*,
	Date(convert_timezone('America/New_York', to_timestamp_tz(c.ISSUED_TIMESTAMP /1000))) AS CANCELLATION_DATE,
	c."NAME" AS CANCELLATION_REASON
FROM
(
SELECT 
	x.POLICY_LOCATOR,
	policy_issued_date,
	policy_start_date,
	policy_start_date_min_issued_effective_date,
	CASE WHEN lead(policy_start_date) OVER (PARTITION BY x.POLICY_LOCATOR ORDER BY policy_start_date) IS NULL 
		THEN Date(convert_timezone('America/New_York', to_timestamp_tz(b.POLICY_END_TIMESTAMP /1000)))
		ELSE (lead(policy_start_date) OVER (PARTITION BY x.POLICY_LOCATOR ORDER BY policy_start_date))-1
	END AS policy_end_date,
	CASE WHEN (rank() OVER (PARTITION BY x.POLICY_LOCATOR ORDER BY policy_start_date)) =1 THEN 'New'
		ELSE 'Renew'
	END AS new_renew,
	rank() OVER (PARTITION BY x.POLICY_LOCATOR ORDER BY policy_start_date) AS POLICY_TERM
from(
SELECT
	POLICY_LOCATOR,
	Date(convert_timezone('America/New_York', to_timestamp_tz(a.ISSUED_TIMESTAMP /1000))) AS policy_issued_date,
	CASE WHEN 
		TYPE ='create' then 
			CASE 
				WHEN a.ISSUED_TIMESTAMP <a.EFFECTIVE_TIMESTAMP then 
					Date(convert_timezone('America/New_York', to_timestamp_tz(a.ISSUED_TIMESTAMP /1000)))
				ELSE
					Date(convert_timezone('America/New_York', to_timestamp_tz(a.EFFECTIVE_TIMESTAMP /1000)))
			END
		ELSE Date(convert_timezone('America/New_York', to_timestamp_tz(EFFECTIVE_TIMESTAMP /1000)))
	END AS policy_start_date_min_issued_effective_date,
	Date(convert_timezone('America/New_York', to_timestamp_tz(EFFECTIVE_TIMESTAMP /1000))) AS policy_start_date
FROM MYSQL_DATA_MART_10001.POLICY_MODIFICATION a
WHERE TYPE IN ('create','renew')
AND a.ISSUED_TIMESTAMP IS NOT NULL
) x
LEFT JOIN MYSQL_DATA_MART_10001."POLICY" b
ON x.POLICY_LOCATOR =b."LOCATOR" 
) xx
LEFT JOIN MYSQL_DATA_MART_10001.CANCELLATION c
ON xx.POLICY_LOCATOR=c.POLICY_LOCATOR 
AND c.STATE ='issued'
AND (Date(convert_timezone('America/New_York', to_timestamp_tz(c.EFFECTIVE_TIMESTAMP /1000))) BETWEEN policy_start_date_min_issued_effective_date AND policy_end_date) 