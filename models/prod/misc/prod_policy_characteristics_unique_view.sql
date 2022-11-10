{{ config(materialized='table') }}

select *
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY created_timestamp desc, start_timestamp desc) num
    FROM {{ ref('prod_policy_characteristics')}}
) inn
where inn.num = 1