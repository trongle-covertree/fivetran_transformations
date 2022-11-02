{{ config(materialized='incremental') }}

{{ run_exposures_perils( env='transformations_dynamodb_develop', prefix='develop' )}}