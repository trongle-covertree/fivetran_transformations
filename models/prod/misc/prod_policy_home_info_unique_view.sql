{{ config(materialized='table') }}

select *
FROM
(
    SELECT id, pk, to_decimal(roof_year_yyyy) as roof_year_yyyy, skirting_type, roof_shape, roof_material, home_hud_number,
        to_decimal(total_square_footage) as total_square_footage, home_type, to_decimal(model_year) as model_year, manufacturer_name,
        roof_condition, home_fixtures, inspection_interior, inspection_exterior, inspection_aerial, created_timestamp, updated_timestamp,
        policy_created_timestamp, policy_updated_timestamp,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY created_timestamp desc) num
    FROM {{ ref('prod_policy_exposures_home_info') }}
) inn
where inn.num = 1