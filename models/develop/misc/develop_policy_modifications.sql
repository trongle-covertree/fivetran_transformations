{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB_DEVELOP.DEVELOP_POLICY_MODIFICATIONS where ID = 'NO FIELDS'") }}

{{ run_modifications( env='transformations_dynamodb_develop', prefix='develop' )}}