{{ config(materialized='incremental') }}

SELECT * FROM transformations_dynamodb_develop.develop.develop_policyholders_locator_sk_entity
UNION
(
{{ run_policyholders(
    env='transformations_dynamodb_develop',
    prefix='develop',
    policyholder_type='LOCATOR'
)}}
)