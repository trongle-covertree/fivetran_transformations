{{ config(materialized='table') }}

SELECT sk AS PK, status, created_timestamp, updated_timestamp
FROM
  ( select distinct sk, status, to_timestamp(to_varchar(created_at)) as created_timestamp, to_timestamp(to_varchar(updated_at)) as updated_timestamp
         , ROW_NUMBER() OVER(partition by sk order by SK, case when status = 'Quote-Expired' or status = 'Initial-Quote' then 1 else 0 end) AS RowNumber
    FROM fivetran_covertree.transformations_dynamodb.prod_quotes t
  ) AS tmp
WHERE RowNumber = 1 
UNION
SELECT PK, STATUS, created_timestamp, updated_timestamp from fivetran_covertree.transformations_dynamodb.prod_policies_policy where status != 'Initial-Quote'