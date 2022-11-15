{{ config(materialized='incremental') }}

{{ run_referrals( env='transformations_dynamodb', prefix='prod', partners_db='dynamodb' )}}