{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICY_MODIFICATIONS where ID = 'NO FIELDS'") }}

{{ run_modifications( env='transformations_dynamodb', prefix='prod' )}}