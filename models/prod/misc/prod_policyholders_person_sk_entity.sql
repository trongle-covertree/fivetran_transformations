{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICYHOLDERS_PERSON_SK_ENTITY where PK = 'NO FIELDS' " ) }}

{{ run_policyholders(
    env='transformations_dynamodb',
    prefix='prod',
    policyholder_type='PERSON'
)}}