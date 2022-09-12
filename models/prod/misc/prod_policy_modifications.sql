{{ config(materialized='table') }}

{{ run_modifications( env='transformations_dynamodb', prefix='prod' )}}