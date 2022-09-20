{{ config(materialized='table') }}

{{ run_exposures_perils( env='transformations_dynamodb_develop', prefix='develop' )}}