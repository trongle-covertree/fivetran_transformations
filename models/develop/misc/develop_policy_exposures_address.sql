{{ config(materialized='incremental', unique_key='id') }}

{{ run_exposures_address( env='transformations_dynamodb_develop', prefix='develop' )}}