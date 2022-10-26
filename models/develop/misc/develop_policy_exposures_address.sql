{{ config(materialized='incremental', unique_key='id', incremental_strategy='delete+insert') }}

{{ run_exposures_address( env='transformations_dynamodb_develop', prefix='develop' )}}