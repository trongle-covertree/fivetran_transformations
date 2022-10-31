{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICY_EXPOSURES_LENDER_INFO where ID = 'NO FIELDS'") }}

{{ run_exposures_lender_info( env='transformations_dynamodb', prefix='prod' )}}