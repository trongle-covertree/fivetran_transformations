{{ config(materialized='incremental') }}

{{ run_exposures_home_info( env='transformations_dynamodb', prefix='prod' )}}