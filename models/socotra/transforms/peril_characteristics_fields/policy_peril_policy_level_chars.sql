{{ config(materialized='table') }}

{{ run_socotra_peril_policy_level_chars('mysql_data_mart_10001', 'transformations_prod_socotra')  }}