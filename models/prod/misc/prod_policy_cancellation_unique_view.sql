{{ config(materialized='table') }}

select *
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY effective_timestamp desc) num
    FROM {{ ref('prod_policies_cancellation') }}
) inn
where inn.num = 1