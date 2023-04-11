{{ config(materialized='incremental') }}

{{ run_peril_modifications( dynamodb_env='transformations_dynamodb', dynamodb_prefix='prod', socotra_env='transformations_prod_socotra' )}}