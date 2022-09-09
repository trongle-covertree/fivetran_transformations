{{ config(materialized='table') }}

{{ run_cancellations( env='transformations_dynamodb', prefix='prod' )}}