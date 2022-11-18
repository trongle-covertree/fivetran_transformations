{{ config(materialized='view') }}

SELECT *
FROM {{ ref('develop_policy_characteristics')}}
where (start_timestamp < current_timestamp() and end_timestamp > current_timestamp() and replaced_timestamp is null) or (start_timestamp = end_timestamp)