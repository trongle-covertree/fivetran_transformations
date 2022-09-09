{{ config(materialized='table') }}

{{ run_policyholders_person( env='transformations_dynamodb_develop', prefix='develop' )}}