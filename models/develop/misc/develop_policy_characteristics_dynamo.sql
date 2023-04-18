{{ config(materialized='incremental') }}

{{ run_characteristics( env='transformations_dynamodb_develop', prefix='develop' )}}