{{ config(materialized='table') }}

select *
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY locator ORDER BY created_timestamp desc, updated_timestamp desc) num
    FROM fivetran_covertree.transformations_dynamodb.prod_policyholders_locator_sk_entity
) inn
where inn.num = 1