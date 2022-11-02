{{ config(materialized='incremental') }}

{{ run_exposures_lender_info( env='transformations_dynamodb', prefix='prod' )}}