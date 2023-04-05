{{ config(materialized='incremental', unique_key=['exposure_characteristics_locator','policy_locator']) }}

{{ run_socotra_exposure_usage('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
