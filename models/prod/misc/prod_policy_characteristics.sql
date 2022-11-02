{{ config(materialized='incremental') }}

{{ run_characteristics( env='transformations_dynamodb', prefix='prod' )}}