{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB_DEVELOP.DEVELOP_POLICY_EXPOSURES_PERILS where PK = 'NO FIELDS'") }}

{{ run_exposures_perils( env='transformations_dynamodb_develop', prefix='develop' )}}