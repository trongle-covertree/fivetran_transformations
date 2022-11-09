{{ config(materialized='view') }}

select *
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY created_timestamp desc, start_timestamp desc) num
    FROM {{ ref('develop_policy_characteristics')}}
) inn
where inn.num = 1