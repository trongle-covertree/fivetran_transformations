{{ config(materialized='table') }}

{{ run_cancellations( env='dynamodb_develop', prefix='' )}}