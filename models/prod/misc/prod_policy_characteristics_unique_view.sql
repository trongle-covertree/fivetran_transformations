{{ config(materialized='table') }}

SELECT *
FROM {{ ref('prod_policy_characteristics')}}
where (start_timestamp < current_timestamp() and end_timestamp > current_timestamp() and replaced_timestamp is null) or (start_timestamp = end_timestamp)