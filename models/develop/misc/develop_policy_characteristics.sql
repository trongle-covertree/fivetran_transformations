{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB_DEVELOP.DEVELOP_POLICY_CHARACTERISTICS where PK = 'NO FIELDS'") }}

{{ run_characteristics( env='transformations_dynamodb_develop', prefix='develop' )}}