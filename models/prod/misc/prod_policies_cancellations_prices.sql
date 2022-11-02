{{ config(materialized='incremental') }}

{{ run_cancellations( env='transformations_dynamodb', prefix='prod' )}}