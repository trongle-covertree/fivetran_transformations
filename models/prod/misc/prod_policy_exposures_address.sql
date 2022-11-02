{{ config(materialized='incremental') }}

{{ run_exposures_address( env='transformations_dynamodb', prefix='prod' )}}