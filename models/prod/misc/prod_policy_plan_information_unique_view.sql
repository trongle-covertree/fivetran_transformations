{{ config(materialized='table',
    post_hook = "UPDATE FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.prod_policy_plan_information_unique_view t1 SET park_name = t2.name FROM (select g.pk as pk, cpl.name as name from FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.communities_partner_lookup as cpl inner join FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.prod_policy_exposures_grid_info as g on g.ct_mhcid = cpl.ct_mhcid) t2 WHERE park_name is null and t1.pk = t2.pk") }}

select *
FROM
(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY created_timestamp desc) num
    FROM {{ ref('prod_policy_exposures_plan_information') }}
) inn
where inn.num = 1