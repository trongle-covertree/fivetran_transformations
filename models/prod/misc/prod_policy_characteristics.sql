{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICY_CHARACTERISTICS where PK = 'NO FIELDS'") }}

{{ run_characteristics( env='transformations_dynamodb', prefix='prod' )}}