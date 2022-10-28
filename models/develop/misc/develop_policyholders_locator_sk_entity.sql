{{ config(materialized='incremental',
    post_hook = "DELETE FROM transformations_dynamodb_develop.develop.develop_policyholders_locator_sk_entity where PK = 'NO FIELDS' ") }}

{{ run_policyholders(
    env='transformations_dynamodb_develop',
    prefix='develop',
    policyholder_type='LOCATOR'
)}}