{{ config(materialized='incremental',
    unique_key=['exposure_characteristics_locator','policy_locator'],
    incremental_strategy='delete+insert') }}

{{ run_socotra_exposure_gen_chars('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
