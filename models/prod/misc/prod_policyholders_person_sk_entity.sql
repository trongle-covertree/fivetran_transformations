{{ config(materialized='incremental') }}

{{ run_policyholders(
    env='transformations_dynamodb',
    prefix='prod',
    policyholder_type='PERSON'
)}}