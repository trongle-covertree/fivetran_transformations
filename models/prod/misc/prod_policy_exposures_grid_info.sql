{{ config(materialized='incremental') }}

{{ run_exposures_grid_info( env='transformations_dynamodb', prefix='prod' )}}