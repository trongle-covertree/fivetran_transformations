{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICY_EXPOSURES_PLAN_INFORMATION where ID = 'NO FIELDS'") }}

{{ run_exposures_plan_information( env='transformations_dynamodb', prefix='prod' )}}