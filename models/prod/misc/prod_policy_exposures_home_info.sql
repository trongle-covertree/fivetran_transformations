{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.PROD_POLICY_EXPOSURES_HOME_INFO where ID = 'NO FIELDS'") }}

{{ run_exposures_home_info( env='transformations_dynamodb', prefix='prod' )}}