{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB_DEVELOP.DEVELOP_POLICY_EXPOSURES_GRID_INFO where ID = 'NO FIELDS'") }}

{{ run_exposures_grid_info( env='transformations_dynamodb_develop', prefix='develop' )}}