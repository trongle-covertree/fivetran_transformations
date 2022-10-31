{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICIES_CANCELLATIONS_PRICES where ID = 'NO FIELDS'") }}

{{ run_cancellations( env='transformations_dynamodb', prefix='prod' )}}