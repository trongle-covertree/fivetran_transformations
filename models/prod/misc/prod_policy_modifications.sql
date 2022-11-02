{{ config(materialized='incremental') }}

{{ run_modifications( env='transformations_dynamodb', prefix='prod' )}}