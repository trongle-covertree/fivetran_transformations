{{ config(materialized='incremental') }}

{{ run_policyholders(
    env='transformations_dynamodb_develop',
    prefix='develop',
    policyholder_type='PERSON'
)}}