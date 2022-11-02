{{ config(materialized='incremental') }}

{{ run_exposures_extra_coverages( env='transformations_dynamodb', prefix='prod' )}}