{{ config(materialized='incremental',
    unique_key=['policy_characteristics_locator', 'policy_locator'],
    incremental_strategy='delete+insert') }}

{{ run_socotra_policy_prior_insurance('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}
