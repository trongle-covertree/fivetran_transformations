{{ config(materialized='table') }}

{{ run_modifications( env='transformations_dynamodb_develop', prefix='develop' )}}