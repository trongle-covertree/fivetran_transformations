{{ config(materialized='incremental',
    unique_key=['policy_characteristics_locator', 'policy_locator'],
    incremental_strategy='delete+insert') }}

{{ run_socotra_policy_level_suppl_uw('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
