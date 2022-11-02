{{ config(materialized='incremental') }}

{{ run_exposures_plan_information( env='transformations_dynamodb_develop', prefix='develop' )}}