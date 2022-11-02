{{ config(materialized='incremental') }}

{{ run_modifications( env='transformations_dynamodb_develop', prefix='develop' )}}