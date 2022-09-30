{{ config(materialized='table') }}

{{ run_cancellations( env='transformations_dynamodb_develop', prefix='develop' )}}