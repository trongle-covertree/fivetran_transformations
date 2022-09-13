{{ config(materialized='table') }}

{{ run_characteristics( env='transformations_dynamodb_develop', prefix='develop' )}}