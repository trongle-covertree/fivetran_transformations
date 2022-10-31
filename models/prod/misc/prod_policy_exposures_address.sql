{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICY_EXPOSURES_ADDRESS where ID = 'NO FIELDS'") }}

{{ run_exposures_address( env='transformations_dynamodb', prefix='prod' )}}