{{ config(materialized='incremental',
    post_hook = "update FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.prod_policy_exposures_grid_info g set ct_mhcid = h.ct_mhcid from FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB.prod_mhcid_historical h where g.pk = h.pk")
}}

{{ run_exposures_grid_info( env='transformations_dynamodb', prefix='prod' )}}