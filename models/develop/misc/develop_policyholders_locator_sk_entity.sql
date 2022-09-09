{{ config(materialized='table') }}

{{ run_policyholders_locator( env='transformations_dynamodb_develop', prefix='develop' )}}