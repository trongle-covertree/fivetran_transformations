{{ config(materialized='table') }}

{{ run_cancellations( env='dynamodb', prefix='prod_' )}}