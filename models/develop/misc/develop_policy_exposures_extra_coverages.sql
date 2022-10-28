{{ config(materialized='incremental') }}

{{ run_exposures_extra_coverages( env='transformations_dynamodb_develop', prefix='develop' )}}