{{ config(materialized='incremental',
    post_hook = "DELETE FROM FIVETRAN_COVERTREE.TRANSFORMATIONS_DYNAMODB_DEVELOP.DEVELOP_POLICY_EXPOSURES_EXTRA_COVERAGES where ID = 'NO FIELDS'") }}

{{ run_exposures_extra_coverages( env='transformations_dynamodb_develop', prefix='develop' )}}