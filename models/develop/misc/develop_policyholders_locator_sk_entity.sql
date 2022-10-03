{{ config(materialized='table') }}

{{ run_policyholders(
    env='transformations_dynamodb_develop',
    prefix='develop',
    policyholder_type='LOCATOR'
)}}