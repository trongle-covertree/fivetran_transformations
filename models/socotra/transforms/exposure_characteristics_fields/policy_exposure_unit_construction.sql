{{ config(materialized='incremental',
    unique_key=['exposure_characteristics_locator','policy_locator', 'policy_modification_locator']) }}

{{ run_socotra_exposure_unit_construction('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
