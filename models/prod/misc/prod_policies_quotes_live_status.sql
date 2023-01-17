{{ config(materialized='table') }}

SELECT PK, status, created_timestamp, updated_timestamp
FROM
  ( select p.pk AS PK, q.status as STATUS, created_timestamp, updated_timestamp from prod_policies_policy as p inner join fivetran_covertree.transformations_dynamodb.prod_quotes as q on p.quote_locator = q.quote_locator where p.status = 'Initial-Quote'
  ) AS tmp
UNION
SELECT PK, STATUS, created_timestamp, updated_timestamp from fivetran_covertree.transformations_dynamodb.prod_policies_policy where status != 'Initial-Quote'