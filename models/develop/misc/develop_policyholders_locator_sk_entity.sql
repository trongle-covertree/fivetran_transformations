{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB_DEVELOP.DEVELOP_POLICYHOLDERS_LOCATOR_SK_ENTITY where PK = 'NO FIELDS' ") }}

{{ run_policyholders(
    env='transformations_dynamodb_develop',
    prefix='develop',
    policyholder_type='LOCATOR'
)}}