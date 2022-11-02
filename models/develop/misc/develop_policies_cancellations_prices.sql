{{ config(materialized='incremental') }}

{{ run_cancellations( env='transformations_dynamodb_develop', prefix='develop' )}}