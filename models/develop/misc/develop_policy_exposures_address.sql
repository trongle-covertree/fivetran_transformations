{{ config(materialized='incremental') }}

{{ run_exposures_address( env='transformations_dynamodb_develop', prefix='develop' )}}